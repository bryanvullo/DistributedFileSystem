import static java.util.Collection.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class Controller {
    
    // port number for Controller
    private static int cport;
    
    // number of replicas
    private static int r;
    
    // timeout for Controller
    private static int timeout;
    
    // time interval for rebalancing
    private static int rebalance_period;
    
    // number of Dstores connected
    private static int num_Dstores;
    
    // key: Dstore port, value: list of files
    private static Index index;
    private static Map<Integer, Socket> dstoreSockets;
    private static HashMap<String, CountDownLatch> latches; //latch, token expected, ports expected
    private static List<ServiceThread> threads;
    
    private static Timer rebalanceTimer;
    private static TimerTask rebalanceTask;
    
    public Controller(int cport, int r, int timeout, int rebalance_period) {
        Controller.cport = cport;
        Controller.r = r;
        Controller.timeout = timeout;
        Controller.rebalance_period = rebalance_period;
        
        Controller.num_Dstores = 0;
        Controller.index = new Index();
        Controller.dstoreSockets = new HashMap<>();
        Controller.latches = new HashMap<>();
    }
    
    public void start() {
        //set the rebalance scheduled task
        rebalanceTimer = new Timer();
        rebalanceTask = new TimerTask() {
            @Override
            public void run() {
                rebalance();
            }
        };
        rebalanceTimer.scheduleAtFixedRate(rebalanceTask, 0, rebalance_period);
        
        // Start the controller
        try {
            ServerSocket ss = new ServerSocket(cport);
            threads = new ArrayList<>();
            for (; ; ) {
                try {
                    Socket client = ss.accept();
                    var thread = new ServiceThread(client);
                    threads.add(thread);
                    thread.start();
                    
                } catch (Exception e) {
                    System.err.println("error in the listening loop:\n" + e);
                }
            }
        } catch (Exception e) {
            System.err.println("error in listening on port: " + cport + "\n" + e);
        }
    }
    
    //any request should be sent over here
    public static void handleRequest(String request, Socket client) {
        var requestWords = request.split(" ");
        
        if (requestWords[0].equals(Protocol.JOIN_TOKEN)) {
            System.out.println("JOIN request received");
            
            int port = Integer.parseInt(requestWords[1]);
            index.port2files.put(port, new ArrayList<>());
            num_Dstores += 1;
            
            try {
                client.setKeepAlive(true);
            } catch (SocketException e) {
                System.err.println("error in setting keep alive for Dstore " + port + ": " + e);
            }
            dstoreSockets.put(port, client);
            
            System.out.println("Dstore " + port + " joined");
            if (num_Dstores > r) {
                System.out.println("Rebalancing...");
                rebalanceTimer.cancel();
                rebalanceTimer.purge();
                rebalanceTask.run();
                rebalanceTimer.scheduleAtFixedRate(rebalanceTask, 0, rebalance_period);
            }
        }
        else if (requestWords[0].equals(Protocol.STORE_TOKEN)) {
            System.out.println("STORE request received");
            var thread = (ServiceThread) Thread.currentThread();
            thread.ongoingCriticalOperation = true;
            
            if (num_Dstores < r) { // not enough Dstores to store the file
                try {
                    var out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    System.out.println("Refusing request as there are not enough DStores");
                } catch (Exception e) {
                    System.err.println(
                        "error in sending ERROR_NOT_ENOUGH_DSTORES request to Client: " + e);
                }
                return;
            }
            
            //parsing the request
            var fileName = requestWords[1];
            var fileSize = Integer.parseInt(requestWords[2]);
            
            synchronized (index) {
                if (index.fileStatus.containsKey(fileName)) { // file already exists
                    try {
                        var out = new PrintWriter(client.getOutputStream(), true);
                        out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        System.out.println("Refusing request as the file already exists");
                    } catch (Exception e) {
                        System.err.println(
                            "error in sending ERROR_FILE_ALREADY_EXISTS request to Client: " + e);
                    }
                    return;
                }
                
                //updating the index
                index.file2ports.put(fileName, new ArrayList<>());
                index.fileStatus.put(fileName, Status.STORING);
                index.fileSizes.put(fileName, fileSize);
            }
            
            //selecting r Dstores to store the file
            var portsToStore = index.port2files.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getValue().size()))
                .limit(r)
                .map(Map.Entry::getKey)
                .toList();
            
            //creating the string to send to Client
            var portsString = portsToStore.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(" "));
            
            //sending the request to the Client
            try {
                System.out.println("sending STORE_TO request to the Client");
                var out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.STORE_TO_TOKEN + " " + portsString);
            } catch (Exception e) {
                System.err.println("error in sending STORE_TO request to Client: " + e);
                index.removeFileStoreFailed(fileName);
            }
            
            //receiving the STORE_ACK from the DStores
            System.out.println("setting up the countdown latch");
            var countdown = new CountDownLatch(r); //countdown latch
            
            //creating latch for the STORE_ACK
            latches.put(Protocol.STORE_ACK_TOKEN + " " + fileName, countdown);
            var storeThread = Thread.currentThread();
            portsToStore.forEach(port -> {
                try {
                    var socket = dstoreSockets.get(port);
                    socket.setSoTimeout(timeout);
                    
                    //creating a thread to listen for the STORE_ACK
                    new Thread(() -> {
                        try {
                            var in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                            var line = in.readLine();
                            System.out.println(line + " received in Thread " + port);
                            
                            if (line == null) {
                                System.out.println("Connection to Dstore " + port + " lost, "
                                    + "not waiting for STORE_ACK");
                                removeDstore(port);
                            }
                            
                            var latch = latches.get(line);
                            if (latch == null)
                                throw new IOException("error in finding the latch for: " + line);
                            else
                                latch.countDown();
                        } catch (SocketTimeoutException e) {
                            System.err.println("timeout in the STORE_ACK Thread for: " + port);
                            storeThread.interrupt();
                        } catch (IOException e) {
                            System.err.println("error in the STORE_ACK Thread for: " + port + e);
                            storeThread.interrupt();
                        }
                    }).start();
                    
                } catch (SocketException e) {
                    System.err.println(
                        "error in setting the timeout for Dstore " + port + ": " + e);
                    index.removeFileStoreFailed(fileName);
                }
            });
            
            System.out.println("Waiting for DStores to respond");
            try {
                countdown.await();
                
                //updating the index after the file has been stored successfully
                index.fileStatus.replace(fileName, Status.STORED);
                portsToStore.forEach(port -> index.file2ports.get(fileName).add(port));
                portsToStore.forEach(port -> index.port2files.get(port).add(fileName));
                latches.remove(Protocol.STORE_ACK_TOKEN + " " + fileName);
                
                System.out.println(
                    "all DStores have responded, sending STORE_COMPLETE request to Client");
                var out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.STORE_COMPLETE_TOKEN);
                
            } catch (InterruptedException e) {
                System.err.println("error in waiting for the countdown latch: " + e);
                index.removeFileStoreFailed(fileName);
            } catch (IOException e) {
                System.err.println("error in sending STORE_COMPLETE request to Client: " + e);
                index.removeFileStoreFailed(fileName);
            }
            
        }
        else if (requestWords[0].equals(Protocol.LOAD_TOKEN)) {
            System.out.println("LOAD request received");
            
            if (num_Dstores < r) { // not enough Dstores to load the file
                try {
                    var out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    System.out.println("Refusing request as there are not enough DStores");
                } catch (Exception e) {
                    System.err.println(
                        "error in sending ERROR_NOT_ENOUGH_DSTORES request to Client: " + e);
                }
                return;
            }
            
            //parsing the request
            var fileName = requestWords[1];
            
            if (index.fileStatus.get(fileName) != Status.STORED) { // file does not exist
                try {
                    var out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    System.out.println("Refusing request as the file does not exist");
                } catch (Exception e) {
                    System.err.println(
                        "error in sending ERROR_FILE_DOES_NOT_EXIST request to Client: " + e);
                }
                return;
            }
            
            //selecting r Dstores to load the file
            var ports = index.file2ports.get(fileName);
            var portsToLoad = new ArrayList<>(ports); //copy of the list
            var fileSize = index.fileSizes.get(fileName);
            
            while (!portsToLoad.isEmpty()) {
                var port = portsToLoad.remove(0);
                
                try {
                    var out = new PrintWriter(client.getOutputStream(), true);
                    var message = Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize;
                    out.println(message);
                    System.out.println("sending LOAD_FROM to the Client: " + message);
                } catch (IOException e) {
                    System.err.println("error in sending LOAD_FROM request to Client: " + e);
                }
                
                //set time out for the client socket in case of a RELOAD request
                try {
                    client.setSoTimeout(timeout);
                } catch (SocketException e) {
                    System.err.println("error in setting the timeout for the Client: " + e);
                }
                
                //if we receive RELOAD request then send the LOAD_FROM again
                try {
                    var in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    var line = in.readLine();
                    System.out.println(line + " received");
                    
                    if (line == null) {
                        System.out.println("Connection to Client closed");
                        return; //exit handleRequest as request has been served
                    }
                    else if (line.equals(Protocol.RELOAD_TOKEN + " " + fileName)) {
                        if (portsToLoad.isEmpty()) {
                            System.out.println("RELOAD request received. "
                                + "No more DStores Available, sending ERROR_LOAD");
                            var out = new PrintWriter(client.getOutputStream(), true);
                            out.println(Protocol.ERROR_LOAD_TOKEN);
                            return;
                        }
                        System.out.println("RELOAD request received, sending LOAD_FROM again");
                    }
                    else {
                        //client has received the file and has sent a new request
                        handleRequest(line, client);
                        return;
                    }
                    
                } catch (SocketTimeoutException e) {
                    //if times out, we assume that the file has been loaded successfully
                    System.out.println("Connection to Client timed out");
                    return; //exit handleRequest as request has been served
                } catch (IOException e) {
                    System.err.println("error in the LOAD_FROM request for: " + port + e);
                }
            }
            
        }
        else if (requestWords[0].equals(Protocol.REMOVE_TOKEN)) {
            System.out.println("REMOVE request received");
            var thread = (ServiceThread) Thread.currentThread();
            thread.ongoingCriticalOperation = true;
            
            if (num_Dstores < r) { // not enough Dstores to remove the file
                try {
                    var out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    System.out.println("Refusing request as there are not enough DStores");
                } catch (Exception e) {
                    System.err.println(
                        "error in sending ERROR_NOT_ENOUGH_DSTORES request to Client: " + e);
                }
                return;
            }
            
            //parsing the request
            var fileName = requestWords[1];
            
            synchronized (index) {
                if (index.fileStatus.get(fileName) != Status.STORED) { // file does not exist
                    try {
                        var out = new PrintWriter(client.getOutputStream(), true);
                        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        System.out.println("Refusing request as the file does not exist");
                    } catch (Exception e) {
                        System.err.println(
                            "error in sending ERROR_FILE_DOES_NOT_EXIST request to Client: " + e);
                    }
                    return;
                }
                
                //updating the index
                index.fileStatus.replace(fileName, Status.REMOVING);
            }
            
            //getting the r DStores to remove the file from
            var portsToRemove = index.file2ports.get(fileName);
            
            //setting up the latch for the REMOVE_ACK
            System.out.println("setting up the countdown latch for REMOVE_ACK");
            var countdown = new CountDownLatch(r); //countdown latch
            latches.put(Protocol.REMOVE_ACK_TOKEN + " " + fileName, countdown);
            
            //removing from DStores
            var removeThread = Thread.currentThread();
            portsToRemove.forEach(port -> { //make a thread for each Dstore
                System.out.println("setting up the Thread for Dstore " + port);
                try {
                    var socket = dstoreSockets.get(port);
                    socket.setSoTimeout(timeout);
                    
                    new Thread(() -> {
                        //sending the REMOVE request
                        try {
                            var out = new PrintWriter(socket.getOutputStream(), true);
                            out.println(request);
                            System.out.println("sending REMOVE to Dstore " + port);
                        } catch (IOException e) {
                            System.err.println(
                                "error in sending REMOVE request to Dstore " + port + ": " + e);
                            removeThread.interrupt();
                        }
                        
                        //listening for the REMOVE_ACK
                        try {
                            System.out.println("listening for REMOVE_ACK from Dstore " + port);
                            var in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                            var line = in.readLine();
                            System.out.println(line + " received in Thread " + port);
                            
                            var latch = latches.get(line);
                            if (latch != null) { //if the latch is found
                                latch.countDown();
                            } else if (line == null) {
                                System.out.println("Connection to Dstore " + port + " lost, "
                                    + "not waiting for REMOVE_ACK");
                                removeDstore(port);
                                portsToRemove.remove(port);
                                latch = latches.get(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                if (latch != null)
                                    latch.countDown();
                            } else if (line.equals(
                                Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName)) {
                                System.out.println(fileName + " does not exist in Dstore " + port);
                                latch = latches.get(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                if (latch != null)
                                    latch.countDown();
                            } else {
                                System.err.println("error in finding the latch for: " + line);
                            }
                        } catch (SocketTimeoutException e) {
                            System.err.println("timeout in the REMOVE_ACK Thread for: " + port);
                            removeThread.interrupt();
                        } catch (IOException e) {
                            System.err.println("error in the REMOVE_ACK Thread for: " + port + e);
                            removeThread.interrupt();
                        }
                    }).start();
                } catch (IOException e) {
                    System.err.println(
                        "error in sending REMOVE request to Dstore " + port + ": " + e);
                }
            });
            
            //waiting for all the REMOVE_ACK
            System.out.println("Waiting for DStores to respond");
            try {
                countdown.await();
                System.out.println("all DStores have responded");
                
                //updating the index after the file has been removed successfully
                System.out.println(
                    "updating the index after the file has been removed successfully");
                index.removeFileRemoveComplete(fileName);
                portsToRemove.forEach(port -> index.port2files.get(port).remove(fileName));
                
                //sending REMOVE_COMPLETE request to the Client
                System.out.println("sending REMOVE_COMPLETE request to Client");
                var out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                latches.remove(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                
            } catch (InterruptedException e) {
                System.err.println("All DStores did not respond in time: " + e);
            } catch (IOException e) {
                System.err.println("error in sending REMOVE_COMPLETE request to Client: " + e);
            }
            
        }
        else if (requestWords[0].equals(Protocol.LIST_TOKEN)) {
            System.out.println("LIST request received");
            
            if (num_Dstores < r) { // not enough Dstores to remove the file
                try {
                    var out = new PrintWriter(client.getOutputStream(), true);
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    System.out.println("Refusing request as there are not enough DStores");
                } catch (Exception e) {
                    System.err.println(
                        "error in sending ERROR_NOT_ENOUGH_DSTORES message to Client: " + e);
                }
                return;
            }
            
            //creating the string to send to Client
            var filesString = index.fileStatus.entrySet().stream()
                .filter(e -> e.getValue() == Status.STORED)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(" "));
            
            try {
                var out = new PrintWriter(client.getOutputStream(), true);
                
                if (filesString.isEmpty()) {
                    System.out.println("sending an empty list of files to the Client");
                    out.println(Protocol.LIST_TOKEN);
                }
                else {
                    System.out.println("sending the list of files to the Client");
                    out.println(Protocol.LIST_TOKEN + " " + filesString);
                }
            } catch (Exception e) {
                System.err.println("error in sending the list of files to the Client: " + e);
            }
            
        }
        else {
            System.err.println("Unknown Request: " + requestWords[0]);
        }
    }
    
    public void rebalance() {
        
        if (num_Dstores < r) {
            System.out.println("Not enough Dstores to Rebalance");
            return;
        }
        
        // wait for all ongoing store/remove operations to finish
        for (var thread : threads) {
            thread.ongoingRebalance = true;
            while (thread.ongoingCriticalOperation) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.err.println("interrupted rebalance: " + e);
                }
            }
        }
        
        // set up for LIST
        var rebalanceThread = Thread.currentThread();
        var countdown = new CountDownLatch(num_Dstores); //countdown latch
        latches.put(Protocol.LIST_TOKEN, countdown);
        var fileList = new ArrayList<String>();
        var dstoreFiles = new HashMap<Integer, List<String>>();
        
        //list sent to all Dstores
        for (var port : dstoreSockets.keySet()) {
            new Thread(() -> {
                try {
                    var socket = dstoreSockets.get(port);
                    socket.setSoTimeout(timeout);
                    if (socket.isClosed()) {
                        System.out.println("Connection to Dstore " + port + " lost, "
                            + "not sending LIST request");
                        removeDstore(port);
                        latches.get(Protocol.LIST_TOKEN).countDown();
                    }
                    
                    System.out.println("sending LIST request to Dstore: " + port);
                    try {
                        var out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(Protocol.LIST_TOKEN);
                    } catch (IOException e) {
                        System.err.println("error in sending LIST request to Dstore: " + port + e);
                    }
                    
                    System.out.println("waiting for LIST response from Dstore: " + port);
                    try {
                        var in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        var line = in.readLine();
                        System.out.println(line + " received from Dstore: " + port);
                        
                        if (line == null) {
                            System.out.println("Connection to Dstore " + port + " lost, "
                                + "not waiting for LIST response");
                            removeDstore(port);
                            latches.get(Protocol.LIST_TOKEN).countDown();
                        }
                        
                        var filesString = line.split(" ");
                        assert filesString[0].equals(Protocol.LIST_TOKEN);
                        var files = List.of(filesString).subList(1, filesString.length);
                        fileList.addAll(files);
                        dstoreFiles.put(port, new ArrayList<>(files));
                        latches.get(Protocol.LIST_TOKEN).countDown();
                        
                    } catch (SocketTimeoutException e) {
                        System.err.println("timeout in the LIST response from Dstore: " + port);
                        latches.get(Protocol.LIST_TOKEN).countDown();
                    } catch (AssertionError e) {
                        System.err.println("DStore " + port + " sent an invalid LIST response: " + e);
                        latches.get(Protocol.LIST_TOKEN).countDown();
                    } catch (IOException e) {
                        System.err.println("error in receiving LIST response from Dstore: " + port + e);
                        latches.get(Protocol.LIST_TOKEN).countDown();
                    }
                    
                } catch (SocketException e) {
                    System.err.println("error in setting the timeout for Dstore: " + port + e);
                    latches.get(Protocol.LIST_TOKEN).countDown();
                }
            }).start();
        }
        
        //wait for all Dstores to respond
        try {
            countdown.await();
            System.out.println("all Dstores have responded to the LIST request");
        } catch (InterruptedException e) {
            System.err.println("error in waiting for the LIST countdown latch: " + e);
        }
        
        //checking if more than r Dstores responded
        if (dstoreFiles.size() < r) {
            System.out.println("Not enough Dstores successfully responded to the LIST request");
            // Rebalance aborted - threads are now free to handle requests
            for (var thread : threads) {
                thread.ongoingRebalance = false;
            }
            return;
        }
        
        //revise file allocation
        var fileAmounts = new HashMap<String, Integer>(); //count how many times each file is stored
        for (var file : fileList) {
            fileAmounts.put(file, fileAmounts.getOrDefault(file, 0) + 1);
        }
        
        var distinctFileList = (ArrayList<String>) fileList.stream().distinct()
            .toList(); //remove duplicates
        
        var fileToRemove = new ArrayList<String>(); //file to completely remove from the system
        
        //checking for files that are not in the index
        for (var file : distinctFileList) {
            if (!index.fileStatus.containsKey(file)) {
                distinctFileList.remove(file);
                fileAmounts.remove(file);
                fileToRemove.add(file);
            }
        }
        
        //checking for files that are in removing status (failed remove operation)
        var indexFilesInRemoving = index.fileStatus.entrySet().stream()
            .filter(e -> e.getValue() == Status.REMOVING)
            .map(Map.Entry::getKey)
            .toList();
        indexFilesInRemoving.forEach((file) -> {
            distinctFileList.remove(file);
            fileAmounts.remove(file);
            fileToRemove.add(file);
        });
        
        //checking for files in index but not in any Dstores
        for (var file : index.fileStatus.keySet()) {
            if (!distinctFileList.contains(file)) {
                index.removeFileRemoveComplete(file); //remove from index
            }
        }
        
        //reviewing what files must we reduced/increased to reach the desired r
        var fileToReduce = new HashMap<String, Integer>();
        var fileToStore = new HashMap<String, Integer>();
        for (var filePair : fileAmounts.entrySet()) {
            if (filePair.getValue() > r) {
                fileToReduce.put(filePair.getKey(), filePair.getValue() - r);
            }
            else if (filePair.getValue() < r) {
                fileToStore.put(filePair.getKey(), r - filePair.getValue());
            }
        }
        
        var f = fileAmounts.size();
        var d = dstoreFiles.size();
        // r
        var maxNum = (int) Math.ceil((double) f * r / d);
        var minNum = (int) Math.floor((double) f * r / d);
        
        //creating the DStore Pairs (files to send, files to remove)
        // dstoreNum -> (files to send, files to remove)
        // files to send = List<Pair<String,List<Integer>>> = list of pairs of file to send, list of ports to send to
        // files to remove = List<String> = list of files to remove
        var dstorePairs = new HashMap<Integer, Pair< ArrayList<Pair<String,ArrayList<Integer>>>, ArrayList<String> >>();
        
        //remove definitely
        var dstoreToRemove = new HashMap<Integer, ArrayList<String>>();
        for (var port : dstoreFiles.keySet()) {
            var files = dstoreFiles.get(port);
            var toRemove = new ArrayList<String>();
            
            for (var file : fileToRemove) {
                if (files.contains(file)) {
                    dstoreFiles.get(port).remove(file);
                    toRemove.add(file);
                }
            }
            dstoreToRemove.put(port, toRemove);
        }
        
        //TODO remove this maybe as the below code should handle this
        // "situation where file is still stored too many times"
        //remove if dstore stores too many & file is stored too many times
        for (var port : dstoreFiles.keySet()) {
            //for each dstore, check if the files is stores more than the maxNum
            for (var file : dstoreFiles.get(port)) {
                if (fileToReduce.containsKey(file) &&
                    fileToReduce.get(file) > 0  &&
                    dstoreFiles.size() > maxNum) {
                    
                    fileToReduce.put(file, fileToReduce.get(file) - 1);
                    dstoreFiles.get(port).remove(file);
                    dstoreToRemove.get(port).add(file);
                }
            }
        }
        
        // situation where file is still stored too many times
        var toReduce = fileToReduce.entrySet().stream()
            .filter(e -> e.getValue() > 0)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        fileToReduce = (HashMap<String, Integer>) toReduce;
        if (!fileToReduce.isEmpty()) {
            for (var entry : fileToReduce.entrySet()) {
                var file = entry.getKey();
                var amount = entry.getValue();
                
                while (amount > 0) {
                    var portsAscending = dstoreFiles.entrySet().stream()
                        .filter(e -> e.getValue().contains(file))
                        .sorted(Comparator.comparing(e -> e.getValue().size()))
                        .map(Map.Entry::getKey)
                        .toList();
                    var ports = portsAscending.reversed();
                    var port = ports.get(0);
                    
                    dstoreFiles.get(port).remove(file);
                    dstoreToRemove.get(port).add(file);
                    amount -= 1;
                }
                
            }
        }
        
        // situation where dstore still stores too many files
        for (var dstorePair : dstoreFiles.entrySet()) {
            var port = dstorePair.getKey();
            var files = dstorePair.getValue();
            
            if (files.size() > maxNum) {
                var toRemove = files.size() - maxNum;
                for (int i = 0; i < toRemove; i++) {
                    var filesAscending = files.stream()
                        .sorted(Comparator.comparing(fileAmounts::get))
                        .toList();
                    var filesDescending = filesAscending.reversed();
                    var file = filesDescending.get(0); //get the file which is stored the most
                    
                    //TODO maybe check if file only stored once
                    
                    fileToStore.merge(file, 1, Integer::sum);
                    dstoreFiles.get(port).remove(file);
                    dstoreToRemove.get(port).add(file);
                }
            }
        }
        
        
        //technique
        // sort the file that need to replicated MOST first
        // then sort the dstores with the least files first
        // algorithm to find from where the files should be sent from
        
        var dstoreToStore = new HashMap<Integer, ArrayList<Pair<String,ArrayList<Integer>>>>();
        //fileToStore
        //dstoreFiles -> length
        for (var entry : fileToStore.entrySet()) {
            var file = entry.getKey();
            var amount = entry.getValue();
            
            for (int i = 0; i < amount; i++) {
                var dstoreToRecieve = dstoreFiles.entrySet().stream()
                    .filter(e -> !e.getValue().contains(file))
                    .sorted(Comparator.comparing(e -> e.getValue().size()))
                    .map(Map.Entry::getKey)
                    .toList()
                    .getFirst();
                
                var dstoreToSend = dstoreFiles.entrySet().stream()
                    .filter(e -> e.getValue().contains(file))
                    .map(Map.Entry::getKey)
                    .toList()
                    .getFirst();
                
                if (dstoreToStore.get(dstoreToSend) == null) {
                    var listToSendPorts = new ArrayList<>(List.of(dstoreToRecieve));
                    var pair = new Pair<>(file, listToSendPorts);
                    var list = new ArrayList<>(List.of(pair));
                    dstoreToStore.put(dstoreToSend, list);
                } else {
                    var list = dstoreToStore.get(dstoreToSend);
                    var pair = list.stream().filter(p -> p.getFirst().equals(file)).toList().getFirst();
                    if (pair == null) {
                        var listToSendPorts = new ArrayList<>(List.of(dstoreToRecieve));
                        var newPair = new Pair<>(file, listToSendPorts);
                        list.add(newPair);
                    } else {
                        pair.getSecond().add(dstoreToRecieve);
                    }
                }
                dstoreFiles.get(dstoreToRecieve).add(file);
            }
        }
        
        
        //Join two maps to create the pairs
        //dstorePairs = dstoreToStore + dstoreToRemove
        var entries = new HashSet<Integer>();
        entries.addAll(dstoreToStore.keySet());
        entries.addAll(dstoreToRemove.keySet());
        for (var entry : entries) {
            var toStore = dstoreToStore.get(entry);
            toStore = toStore == null ? new ArrayList<>() : toStore;
            var toRemove = dstoreToRemove.get(entry);
            toRemove = toRemove == null ? new ArrayList<>() : toRemove;
            var pair = new Pair<>(toStore, toRemove);
            dstorePairs.put(entry, pair);
        }
        
        //set up the countdown latch for the REBALANCE_COMPLETE
        System.out.println("setting up the countdown latch for REBALANCE_COMPLETE");
        countdown = new CountDownLatch(dstorePairs.size()); //countdown latch
        
        //creating latch for the STORE_ACK
        latches.put(Protocol.REMOVE_COMPLETE_TOKEN, countdown);
        
        //sending the rebalance request to the Dstores
        for (var entry : dstorePairs.entrySet()) {
            var dstore = entry.getKey();
            var pair = entry.getValue();
            
            //extracting the data
            var toStore = pair.getFirst();
            var amountToStore = toStore.size();
            var toRemove = pair.getSecond();
            var amountToRemove = toRemove.size();
            
            //creating the string to send to Dstore
            var messageBuilder = new StringBuilder(
                Protocol.REBALANCE_TOKEN + " " + amountToStore);
            for (var filePair : toStore) { //adding the files to send to other dstores
                var file = filePair.getFirst();
                var ports = filePair.getSecond();
                messageBuilder.append(" ").append(file).append(" ").append(ports.size());
                for (var port : ports) {
                    messageBuilder.append(" ").append(port);
                }
            }
            messageBuilder.append(" ").append(amountToRemove);
            for (var file : toRemove) { //adding the files to remove from dstore
                messageBuilder.append(" ").append(file);
            }
            var message = messageBuilder.toString();
            
            //sending the request to the Dstore
            new Thread(() -> {
                try {
                    var socket = dstoreSockets.get(dstore);
                    socket.setSoTimeout(timeout);
                    
                    System.out.println("Sending (" + message + ") to Dstore: " + dstore);
                    try {
                        var out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(message);
                    } catch (IOException e) {
                        System.err.println("error in sending REBALANCE request to Dstore: " + dstore + e);
                    }
                    
                    System.out.println("waiting for REBALANCE_COMPLETE response from Dstore: " + dstore);
                    try {
                        var in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        var line = in.readLine();
                        System.out.println(line + " received from Dstore: " + dstore);
                        
                        if (line == null) {
                            System.out.println("Connection to Dstore " + dstore + " lost, "
                                + "not waiting for REBALANCE_COMPLETE response");
                            removeDstore(dstore);
                            rebalanceThread.interrupt();
                        } else if (!line.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                            System.err.println("DStore " + dstore + " sent an invalid REBALANCE_COMPLETE response: " + line);
                            rebalanceThread.interrupt();
                        } else {
                            latches.get(Protocol.REBALANCE_COMPLETE_TOKEN).countDown();
                        }
                        
                    } catch (SocketTimeoutException e) {
                        System.err.println("timeout in the REBALANCE_COMPLETE response from Dstore: " + dstore);
                        rebalanceThread.interrupt();
                    }  catch (IOException e) {
                        System.err.println(
                            "error in receiving REBALANCE_COMPLETE response from Dstore: " + dstore + e);
                        rebalanceThread.interrupt();
                    }
                    
                } catch (SocketException e) {
                    System.err.println("error in setting the timeout for Dstore: " + dstore + e);
                    rebalanceThread.interrupt();
                }
            });
            
        }
        
        //waiting for responses from the Dstores
        try {
            countdown.await();
            System.out.println("all Dstores have responded to the REBALANCE request, updating Index");
            index.update(dstoreFiles);
        } catch (InterruptedException e) {
            System.err.println("Rebalance Thread Interrupted, not updating the Index: " + e);
        }
        
        // Rebalance complete - threads are now free to handle requests
        for (var thread : threads) {
            thread.ongoingRebalance = false;
        }
    }
    
    private static void removeDstore(int port) {
        new Thread(() -> index.removePort(port)).start();
        num_Dstores -= 1;
        dstoreSockets.remove(port);
    }
    
    static class ServiceThread extends Thread {
        
        Socket client;
        boolean ongoingRebalance = false;
        boolean ongoingCriticalOperation = false;
        
        Queue<String> requests = new LinkedList<>();
        
        ServiceThread(Socket c) {
            client = c;
        }
        
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream()));
                String line;
                boolean isDstore = false;
                
                while ((line = in.readLine()) != null || (!requests.isEmpty() && !this.ongoingRebalance)) {
                    
                    if (!requests.isEmpty() && !this.ongoingRebalance) {
                        System.out.println("Queue not empty, handling requests");
                        for (var request : requests) {
                            handleRequest(request, client);
                        }
                        requests.clear();
                        if (line == null) {
                            continue;
                        }
                    }
                    
                    System.out.println(line + " received");
                    if (this.ongoingRebalance) {
                        System.out.println("Rebalance ongoing, adding request to queue");
                        requests.add(line);
                        continue;
                    }
                    
                    if (line.split(" ")[0].equals(Protocol.JOIN_TOKEN)) {
                        handleRequest(line, client);
                        isDstore = true;
                        break; //stop listening for more requests from DStore
                    }
                    
                    handleRequest(line, client);
                    ongoingCriticalOperation = false;
                }
                
                if (!isDstore) { //if the client is not a Dstore then close connection
                    client.close();
                }
                
            } catch (Exception e) {
                System.err.println("error in the Thread listening loop:\n" + e);
                
            }
        }
    }
    
    // command prompt: java Controller cport R timeout rebalance_period
    // test command prompt: java Controller 4321 1 2000 10000
    // client command prompt: java -cp client.jar:. ClientMain 4321 1000
    // client compile: javac -cp client.jar ClientMain.java
    public static void main(String[] args) {
        //TODO validate arguments
        int cport = Integer.parseInt(args[0]);
        int r = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);
        
        Controller controller = new Controller(cport, r, timeout, rebalance_period);
        controller.start();
    }
}
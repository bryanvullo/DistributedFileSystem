import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
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
    private final int rebalance_period;
    
    // number of Dstores connected
    private static int num_Dstores;
    
    // key: Dstore port, value: list of files
    private static Index index;
    private static Map<Integer, Socket> dstoreSockets;
    private static HashMap<String, CountDownLatch> latches; //latch, token expected, ports expected
    
    public Controller(int cport, int r, int timeout, int rebalance_period) {
        Controller.cport = cport;
        Controller.r = r;
        Controller.timeout = timeout;
        this.rebalance_period = rebalance_period;
        
        Controller.num_Dstores = 0;
        Controller.index = new Index();
        Controller.dstoreSockets = new HashMap<>();
        Controller.latches = new HashMap<>();
    }
    
    public void start() {
        // Start the controller
        try {
            ServerSocket ss = new ServerSocket(cport);
            for (; ; ) {
                try {
                    Socket client = ss.accept();
                    new Thread(new ServiceThread(client)).start();
                    
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
                //TODO implement rebalancing
            }
        }
        else if (requestWords[0].equals(Protocol.STORE_TOKEN)) {
            System.out.println("STORE request received");
            
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
                            
                            var latch = latches.get(line);
                            if (latch == null)
                                System.err.println("error in finding the latch for: " + line);
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
                System.out.println(
                    "all DStores have responded, sending STORE_COMPLETE request to Client");
                var out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.STORE_COMPLETE_TOKEN);
                latches.remove(Protocol.STORE_ACK_TOKEN + " " + fileName);
                
                //updating the index after the file has been stored successfully
                index.fileStatus.replace(fileName, Status.STORED);
                portsToStore.forEach(port -> index.file2ports.get(fileName).add(port));
                portsToStore.forEach(port -> index.port2files.get(port).add(fileName));
                
            } catch (InterruptedException e) {
                System.err.println("error in waiting for the countdown latch: " + e);
                index.removeFileStoreFailed(fileName);
            } catch (IOException e) {
                System.err.println("error in sending STORE_COMPLETE request to Client: " + e);
                index.removeFileStoreFailed(fileName);
            }
            
        }
        else if (requestWords[0].equals(Protocol.STORE_ACK_TOKEN)) {} //do nothing
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
                            System.err.println("error in sending REMOVE request to Dstore " + port + ": " + e);
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
                            }
                            else if (line.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName)) {
                                System.out.println(fileName + " does not exist in Dstore " + port);
                                latch = latches.get(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                if (latch != null) latch.countDown();
                            }
                            else {
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
                    System.err.println("error in sending REMOVE request to Dstore " + port + ": " + e);
                }
            });
            
            //waiting for all the REMOVE_ACK
            System.out.println("Waiting for DStores to respond");
            try {
                countdown.await();
                System.out.println("all DStores have responded");
                
                //updating the index after the file has been removed successfully
                System.out.println("updating the index after the file has been removed successfully");
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
            System.err.println("unknown request: " + requestWords[0]);
        }
    }
    
    static class ServiceThread implements Runnable {
        
        Socket client;
        
        ServiceThread(Socket c) {
            client = c;
        }
        
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream()));
                String line;
                boolean isDstore = false;
                
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                    
                    if (line.split(" ")[0].equals(Protocol.JOIN_TOKEN)) {
                        handleRequest(line, client);
                        isDstore = true;
                        break; //stop listening for more requests from DStore
                    }
                    
                    handleRequest(line, client);
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
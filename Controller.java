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
            
            if (index.file2ports.containsKey(fileName)) { // file already exists
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
            index.fileStatus.put(fileName, Index.Status.STORING);
            
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
                index.fileStatus.remove(fileName);
                index.file2ports.remove(fileName);
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
                    index.fileStatus.remove(fileName);
                    index.file2ports.remove(fileName);
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
                index.fileStatus.replace(fileName, Index.Status.STORED);
                portsToStore.forEach(port -> index.file2ports.get(fileName).add(port));
                portsToStore.forEach(port -> index.port2files.get(port).add(fileName));
                
            } catch (InterruptedException e) {
                System.err.println("error in waiting for the countdown latch: " + e);
                index.fileStatus.remove(fileName);
                index.file2ports.remove(fileName);
            } catch (IOException e) {
                System.err.println("error in sending STORE_COMPLETE request to Client: " + e);
                index.fileStatus.remove(fileName);
                index.file2ports.remove(fileName);
            }
            
        }
        else if (requestWords[0].equals(Protocol.STORE_ACK_TOKEN)) {} //do nothing
        else if (requestWords[0].equals(Protocol.LOAD_TOKEN)) {
        
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
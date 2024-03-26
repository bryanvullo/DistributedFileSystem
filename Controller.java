import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Controller {
    
    // port number for Controller
    private static int cport;
    
    // number of replicas
    private static int r;
    
    // timeout for Controller
    private final int timeout;
    
    // time interval for rebalancing
    private final int rebalance_period;
    
    // number of Dstores connected
    private static int num_Dstores;
    
    // key: Dstore port, value: list of files
    private static Index index;
    
    public Controller(int cport, int r, int timeout, int rebalance_period) {
        Controller.cport = cport;
        Controller.r = r;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        
        Controller.num_Dstores = 0;
        Controller.index = new Index();
    }
    
    public void start() {
        // Start the controller
        try{
            ServerSocket ss = new ServerSocket(cport);
            for (;;) {
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
        //TODO implement
        var requestWords = request.split(" ");
        System.out.println(requestWords[0] + ":" + requestWords[1]);
        
        if (requestWords[0].equals(Protocol.JOIN_TOKEN)) {
            System.out.println("JOIN request received");
            
            int port = Integer.parseInt(requestWords[1]);
            index.port2files.put(port, new ArrayList<>());
            num_Dstores += 1;
            
            System.out.println("Dstore " + port + " joined");
            if (num_Dstores > r) {
                System.out.println("Rebalancing...");
                //TODO implement rebalancing
            }
        } else if (requestWords[0].equals(Protocol.STORE_TOKEN)) {
            System.out.println("STORE request received");
            
            if (num_Dstores < r) return; // not enough Dstores to store the file
            
            //parsing the request
            var fileName = requestWords[1];
            var fileSize = Integer.parseInt(requestWords[2]);
            
            //updating the index
            index.file2ports.put(fileName, new ArrayList<>());
            index.fileStatus.put(fileName, Index.Status.STORING);
            
            //selecting r Dstores to store the file
            var portsToStore = index.port2files.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getValue().size()))
                .limit(r)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            
            //creating the string to send to Client
            var portsString = portsToStore.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(" "));
            
            //sending the request to the Client
            try {
                var out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.STORE_TO_TOKEN + " " + portsString);
            } catch (Exception e) {
                System.err.println("error in sending STORE_TO request to Client: " + e);
            }
            
            //receiving the ACK from the DStores
            //countdown latch
            //timeout
            
        }
        //else if (requestWords[0].equals(Protocol.LOAD_TOKEN)) {
//            System.out.println("LOAD request received");
//
//            String file = requestWords[1];
//
//            if (index.containsKey(port)) {
//                index.get(port).add(file);
//                System.out.println("File " + file + " added to Dstore " + port);
//            } else {
//                System.out.println("Dstore " + port + " not found");
//            }
//        } else if (requestWords[0].equals(Protocol.REMOVE_TOKEN)) {
//            System.out.println("REMOVE request received");
//
//            int port = Integer.parseInt(requestWords[1]);
//            String file = requestWords[2];
//
//            if (index.containsKey(port)) {
//                index.get(port).remove(file);
//                System.out.println("File " + file + " removed from Dstore " + port);
//            } else {
//                System.out.println("Dstore " + port + " not found");
//            }
//        }
    }
    
    static class ServiceThread implements Runnable{
        Socket client;
        
        ServiceThread(Socket c) {
            client = c;
        }
        
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream()));
                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                    handleRequest(line, client);
                }
                
                client.close();
            
            }
            catch (Exception e) {
                System.err.println("error in the Thread listening loop:\n" + e);
                
            }
        }
    }
    
    // command prompt: java Controller cport R timeout rebalance_period
    // test command prompt: java Controller 4321 3 1000 10000
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Controller {
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalance_period;
    
    private int num_Dstores;
    private Map<Integer, List<String>> index;
    
    public Controller(int cport, int r, int timeout, int rebalance_period) {
        this.cport = cport;
        this.r = r;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        
        this.num_Dstores = 0;
        this.index = new HashMap<>();
    }
    
    public void start() {
        // Start the controller
        try{
            ServerSocket ss = new ServerSocket(cport);
            for (;;) {
                try {
                    Socket client = ss.accept();
                    
                    //TODO pass the request to a new thread
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println(line + " received");
                        handleRequest(line);
                    }
                    
                    client.close();
                    
                } catch (Exception e) {
                    System.out.println("error in the listening loop: "+e);
                }
            }
        } catch (Exception e) {
            System.err.println("error in listening on port: " + cport + " " + e);
        }
    }
    
    public void handleRequest(String request) {
        //TODO implement
        var requestWords = request.split(" ");
        System.out.println(requestWords[0] + ":" + requestWords[1]);
        
        if (requestWords[0].equals(Protocol.JOIN_TOKEN)) {
            System.out.println("JOIN request received");
            
            int port = Integer.parseInt(requestWords[1]);
            index.put(port, new ArrayList<>());
            num_Dstores += 1;
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

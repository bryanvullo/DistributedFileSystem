import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Controller {
    int cport;
    int r;
    int timeout;
    int rebalance_period;
    
    int num_Dstores;
    Map<Integer, List<String>> index;
    
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
                try {Socket client = ss.accept();
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                    String line;
                    while ((line = in.readLine()) != null)
                        System.out.println(line+" received");
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
        switch (requestWords[0]) {
            case "JOIN":
                //TODO implement
                System.out.println("JOIN request received");
                break;
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

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class Dstore {
    int port;
    int cport;
    int timeout;
    String file_folder;
    
    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }
    
    public void start() {
        // Start the Dstore
        try {
            Socket socket = new Socket(InetAddress.getLocalHost(), cport);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            
            out.println("JOIN " + port);
            out.flush();
            
            //testing
            Thread.sleep(3000);
            out.close();
            
        } catch (Exception e) {
            System.out.println("error trying to connect to Controller: " + e);
        }
    }
    
    // command promptL java Dstore port cport timeout file_folder
    // test command prompt: java Dstore 4322 4321 1000 /tmp/dstore1
    public static void main(String[] args) {
        //TODO validate arguments
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        
        Dstore dstore = new Dstore(port, cport, timeout, file_folder);
        dstore.start();
    }

}

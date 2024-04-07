import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
    int port;
    int cport;
    int timeout;
    String file_folder;
    Socket controllerSocket;
    
    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }
    
    public void start() {
        // Start the Dstore
        try {
            controllerSocket = new Socket(InetAddress.getLocalHost(), cport);
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            
            out.println(Protocol.JOIN_TOKEN + " " + port);
            
            listen();
            
        } catch (Exception e) {
            System.out.println("error trying to connect to Controller: " + e);
        }
    }
    
    public void listen() {
        // listen for incoming connections
        try {
            ServerSocket socket = new ServerSocket(port);
            for (;;) {
                try {
                    Socket client = socket.accept();
                    
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println(line + " received");
                        handleRequest(line, client);
                    }
                    
                    System.out.println("closing connection");
                    client.close();
                    
                } catch (Exception e) {
                    System.err.println("error in the listening loop:\n" + e);
                }
            }
        } catch (Exception e) {
            System.err.println("error in listening on port: " + port + "\n" + e);
        }
    }
    
    public void handleRequest(String request, Socket client) {
        //TODO handle the request
        var requestWords = request.split(" ");
        
        if (requestWords[0].equals(Protocol.STORE_TOKEN)) {
            System.out.println("STORE request received");
            
            var fileName = requestWords[1];
            var fileSize = Integer.parseInt(requestWords[2]);
            
            //sending ACK
            try {
                System.out.println("sending ACK to client");
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.ACK_TOKEN);
            } catch (Exception e) {
                System.out.println("error sending ACK: " + e);
            }
            
            //receiving the file
            var buffer = new byte[fileSize];
            try {
                System.out.println("receiving the file from the client");
                var in = client.getInputStream(); //using the same socket
                buffer = in.readNBytes(fileSize);
            } catch (Exception e) {
                System.out.println("error receiving the file: " + e);
            }
            
            //saving the file
            try {
                System.out.println("saving the file");
                var out = new FileOutputStream(file_folder + "/" + fileName);
                out.write(buffer);
                
                out.close(); //close the file stream
            } catch (Exception e) {
                System.out.println("error saving the file: " + e);
            }
            
            //sending STORE_ACK
            try {
                System.out.println("sending STORE_ACK to the Controller: " + fileName);
                PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
                out.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
            } catch (Exception e) {
                System.out.println("error sending STORE_ACK: " + e);
            }
        }
        else if (requestWords[0].equals(Protocol.LOAD_DATA_TOKEN)) {
            System.out.println("LOAD_DATA request received");
            
            var fileName = requestWords[1];
            
            //sending the file
            try {
                System.out.println("sending the file " + fileName + " to the client");
                var out = client.getOutputStream(); //using the same socket
                var in = new File(file_folder + "/" + fileName);
                
                if (!in.exists()) {
                    System.out.println("file does not exist: " + fileName);
                    client.close();
                    return;
                }
                
                var buffer = new byte[(int) in.length()];
                var fis = new FileInputStream(in);
                fis.read(buffer);
                fis.close();
                out.write(buffer);
            } catch (Exception e) {
                System.out.println("error sending the file: " + e);
            }
        }
        else if (requestWords[0].equals(Protocol.REMOVE_TOKEN)) {
            System.out.println("REMOVE request received");
            
            var fileName = requestWords[1];
            
            //removing the file
            try {
                System.out.println("removing the file " + fileName);
                var out = new PrintWriter(controllerSocket.getOutputStream(), true);
                var in = new File(file_folder + "/" + fileName);
                
                if (!in.exists()) {
                    System.out.println("file does not exist: " + fileName);
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                } else {
                    in.delete();
                    out.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                }
                
            } catch (IOException e) {
                System.out.println("error removing the file: " + e);
            }
            
        }
        else {
            System.out.println("unknown request received: " + request);
        }
    }
    
    // command promptL java Dstore port cport timeout file_folder
    // test command prompt: java Dstore 4322 4321 1000 tmp/dstore1
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

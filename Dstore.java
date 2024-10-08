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
import java.util.HashMap;
import java.util.Map;

public class Dstore {
    int port;
    int cport;
    int timeout;
    String file_folder;
    Map<String,Integer> fileSizes;
    Socket controllerSocket;
    
    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
        this.fileSizes = new HashMap<>();
    }
    
    public void start() {
        // Start the Dstore
        try {
            controllerSocket = new Socket(InetAddress.getLocalHost(), cport);
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            
            out.println(Protocol.JOIN_TOKEN + " " + port);
            
            new Thread(this::listenToController).start();
            
            listen();
            
        } catch (Exception e) {
            System.out.println("error trying to connect to Controller: " + e);
        }
    }
    
    public void listenToController() {
        // listen for incoming requests from the controller
        for (;;) {
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(controllerSocket.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received from Controller");
                    handleRequest(line, controllerSocket);
                }
            } catch (Exception e) {
                System.err.println("error in the listening loop for Controller:\n" + e);
            }
        }
    }
    
    public void listen() {
        // listen for incoming connections from clients
        try {
            ServerSocket socket = new ServerSocket(port);
            for (;;) {
                try {
                    Socket client = socket.accept();
                    
                    new Thread(new ClientThread(client)).start();
                    
                } catch (Exception e) {
                    System.err.println("error in the listening loop:\n" + e);
                }
            }
        } catch (Exception e) {
            System.err.println("error in listening on port: " + port + "\n" + e);
        }
    }
    
    public void handleRequest(String request, Socket client) {
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
                fileSizes.put(fileName, fileSize);
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
                
                var fileSize = fileSizes.get(fileName);
                
                var buffer = new byte[fileSize];
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
                var out = new PrintWriter(controllerSocket.getOutputStream(), true);
                var in = new File(file_folder + "/" + fileName);
                
                if (!in.exists()) {
                    System.out.println("file does not exist: " + fileName);
                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                } else {
                    System.out.println("file exists: " + fileName + ", removing it");
                    in.delete();
                    out.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                }
                
            } catch (IOException e) {
                System.out.println("error removing the file: " + e);
            }
            
        }
        else if (requestWords[0].equals(Protocol.REBALANCE_TOKEN)) {
            System.out.println("REBALANCE request received");
            var counter = 1;
            
            //sending the files
            var filesToSendAmount = Integer.parseInt(requestWords[counter]);
            counter++;
            for (int i = 0; i < filesToSendAmount; i++) {
                var fileName = requestWords[counter];
                counter++;
                
                var file = new File(file_folder + "/" + fileName);
                
                if (!file.exists()) {
                    System.err.println("file does not exist: " + fileName);
                    continue;
                }
                
                var fileSize = fileSizes.get(fileName);
                
                var dstoresToSendAmount = Integer.parseInt(requestWords[counter]);
                counter++;
                
                for (int j = 0; j < dstoresToSendAmount; j++) {
                    var dstorePort = Integer.parseInt(requestWords[counter]);
                    counter++;
                    try {
                        var socket = new Socket(InetAddress.getLocalHost(), dstorePort);
                        socket.setSoTimeout(timeout);
                        var printer = new PrintWriter(socket.getOutputStream(), true);
                        var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        var out = socket.getOutputStream();
                        
                        //sending Rebalance_Store message
                        System.out.println("sending REBALANCE_STORE to Dstore: " + dstorePort);
                        printer.println(Protocol.REBALANCE_STORE_TOKEN + " " + fileName + " " + fileSize);
                        
                        //receiving ack
                        var line = reader.readLine();
                        System.out.println("received: " + line + " from Dstore: " + dstorePort);
                        if (!line.equals(Protocol.ACK_TOKEN)) {
                            System.err.println("Did not receive ACK from Dstore: " + dstorePort);
                            continue;
                        }
                        
                        //sending the file
                        System.out.println("sending the file " + fileName + " to the Dstore: " + dstorePort);
                        var buffer = new byte[fileSize];
                        var fis = new FileInputStream(file);
                        fis.read(buffer);
                        fis.close();
                        out.write(buffer);
                    
                    } catch (Exception e) {
                        System.out.println("error connecting to Dstore: " + dstorePort + "\n" + e);
                    }
                }
            }
            
            //removing the files
            var filesToRemoveAmount = Integer.parseInt(requestWords[counter]);
            counter++;
            for (int i = 0; i < filesToRemoveAmount; i++) {
                var fileName = requestWords[counter];
                counter++;
                
                var file = new File(file_folder + "/" + fileName);
                
                if (!file.exists()) {
                    System.err.println("file does not exist: " + fileName);
                    continue;
                }
                
                file.delete();
            }
            
            //sending REBALANCE_COMPLETE
            try {
                var out = new PrintWriter(controllerSocket.getOutputStream(), true);
                out.println(Protocol.REBALANCE_COMPLETE_TOKEN);
            } catch (Exception e) {
                System.out.println("error sending REBALANCE_COMPLETE: " + e);
            }
            
        }
        else if (requestWords[0].equals(Protocol.REBALANCE_STORE_TOKEN)) {
            System.out.println("REBALANCE_STORE request received");
            
            var fileName = requestWords[1];
            var fileSize = Integer.parseInt(requestWords[2]);
            
            //sending ACK
            try {
                System.out.println("sending ACK to Dstore");
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                out.println(Protocol.ACK_TOKEN);
            } catch (Exception e) {
                System.out.println("error sending ACK: " + e);
                return;
            }
            
            //receiving the file
            var buffer = new byte[fileSize];
            try {
                client.setSoTimeout(timeout);
                System.out.println("receiving the file from the dstore");
                var in = client.getInputStream();
                buffer = in.readNBytes(fileSize);
            } catch (Exception e) {
                System.out.println("error receiving the file: " + e);
                return;
            }
            
            //saving the file
            try {
                System.out.println("saving the file");
                fileSizes.put(fileName, fileSize);
                var out = new FileOutputStream(file_folder + "/" + fileName);
                out.write(buffer);
                
                out.close(); //close the file stream
            } catch (Exception e) {
                System.out.println("error saving the file: " + e);
            }
            
        }
        else if (requestWords[0].equals(Protocol.LIST_TOKEN)) {
            System.out.println("LIST request received");
            
            var files = new File(file_folder).list();
            try {
                assert files != null;
                var out = new PrintWriter(controllerSocket.getOutputStream(), true);
                //creating the list of files message
                var messageBuilder = new StringBuilder();
                messageBuilder.append(Protocol.LIST_TOKEN);
                for (var file : files) {
                    messageBuilder.append(" ");
                    messageBuilder.append(file);
                }
                var message = messageBuilder.toString();
                out.println(message);
                
            } catch (Exception e) {
                System.out.println("error sending the file list to Controller: " + e);
            }
        }
        else {
            System.out.println("unknown request received: " + request);
        }
    }
    
     class ClientThread implements Runnable {
        Socket client;
        
        public ClientThread(Socket client) {
            this.client = client;
        }
        
        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                    handleRequest(line, client);
                }
                client.close();
            } catch (Exception e) {
                System.err.println("error in the listening loop for client:"
                    + client.getPort() + "\n" + e);
            }
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

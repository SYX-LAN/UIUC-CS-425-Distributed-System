import org.unix4j.Unix4j;
import org.unix4j.line.Line;
import org.unix4j.unix.Grep;
import org.unix4j.unix.cut.CutOption;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

public class Server {
    private final ServerSocket serverSocket;
    public Server() {
        try {
            // initialize server, port set to be 9999
            int port = 9999;
            this.serverSocket = new ServerSocket(port);
            System.out.println("Server Starts Successfully!");
        } catch (IOException e) {
            System.out.println("Server Initialization Error");
            throw new RuntimeException(e);
        }
    }

    //
    public void StartListen() {
        while (true) {
            try {
                //try to receive socket from client
                Socket client = serverSocket.accept();

                InputStream inputStream = client.getInputStream();
                ObjectInputStream ops = new ObjectInputStream(inputStream);
                // usr List to store grep result
                List<String> result = new LinkedList<>();
                try {
                    //get grep parameters from Object
                    GrepStruct grep = (GrepStruct) ops.readObject();
                    System.out.println("Server Object read success");
                    result = LogSearch(grep);
                } catch (ClassNotFoundException e) {
                    System.out.println("Server ClassNotFound");
                }
                OutputStream os = client.getOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);
                // transmit List to Client
                oos.writeObject(result);
                oos.flush();

                // close stream
                os.close();
                inputStream.close();
                ops.close();
                oos.close();
            } catch (IOException e) {
                System.out.println("Server IO read exception");
            }
        }
    }

    public List<String> LogSearch(GrepStruct grep) {
        List<String> result = LogSearch.search(grep);
        System.out.println("ready to transmit");
        return result;
    }

    public static void main(String[] args) throws IOException {
        Server server = new Server();
        server.StartListen();
    }
}

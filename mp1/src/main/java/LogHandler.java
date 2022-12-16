import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

/*
 * LogHandler class is implemented as callable class, on which each thread
 * will execute to query a particular machine.
 */
public class LogHandler implements Callable<MachineStats> {

    private String regexp;
    private String option;
    private String serverIP;
    final int serverPort = 9999;
    private Instant start;

    public LogHandler(String serverIP, int serverPort, String regexp, String option) {
        this.serverIP = serverIP;
        this.regexp = regexp;
        this.option = option;
    }

    @Override
    public MachineStats call() {
        // Record the start of execution to measure execution time
        start = Instant.now();
        // Initiate query to distributed machines
        try {
            // If localhost is equal to the host being connected, directly search local file
//            if (serverIP.equals(InetAddress.getLocalHost().getHostAddress()))
//                return localLogSearch();
            // Connect to server
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(serverIP, serverPort));
            socket.setSoTimeout(50 * 1000);
            // Send to server client's query information
            OutputStream outputStream = socket.getOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            GrepStruct grep = new GrepStruct(option, regexp);
            objectOutputStream.writeObject(grep);
            objectOutputStream.flush();
            // Receive query results from the machine
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            List<String> log = (List<String>)objectInputStream.readObject();
            // Write query results to local file
            writeLogFile(log);
            // Obtain line counts
            int lineCount = calculateLineCount(log);
            return new MachineStats(0, lineCount, Duration.between(start, Instant.now()));
        } catch (SocketTimeoutException e) {
            // Socket read timeout
            return new MachineStats(1, 0, Duration.between(start, Instant.now()));
        } catch (IOException | ClassNotFoundException e) {
            // Connection failed
            return new MachineStats(2, 0, Duration.between(start, Instant.now()));
        }
    }

    public void writeLogFile(List<String> log) throws IOException {
        if (log == null) {
            System.out.println("no result");
            return;
        }
        String path = new File(new File("").getAbsolutePath()) + "/log";
        Path dict = Paths.get(path);
        if (!Files.exists(dict)) Files.createDirectory(dict);
        BufferedWriter br = new BufferedWriter(new FileWriter(new File(path + "/machine" + log.get(0).charAt(8) +".txt")));
        for (String str: log.subList(1, log.size())) {
            br.write(str + System.lineSeparator());
        }
        br.flush();
    }

    public int calculateLineCount(List<String> log) {
        int lineCount;
        if (option.equals("-c") || option.equals("-Ec")) {
            lineCount = Integer.parseInt(log.get(1));
        }
        else lineCount = log.size() - 1;
        return lineCount;
    }

    public MachineStats localLogSearch() throws IOException {
        List<String> log = LogSearch.search(new GrepStruct(option, regexp));
        writeLogFile(log);
        int lineCount = calculateLineCount(log);
        return new MachineStats(0, lineCount, Duration.between(start, Instant.now()));
    }
}

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import static java.lang.System.exit;

/*
 * Client class initiates query for distributed log files
 */
public class Client {
    // IP and hostname should be ordered with machine number
    private final String[] IPs = {
            "172.22.156.182", "172.22.158.182",
            "172.22.94.182", "172.22.156.183",
            "172.22.158.183", "172.22.94.183",
            "172.22.156.184", "172.22.158.184",
            "172.22.94.184", "172.22.156.185"
    };
    private final String[] hostname = {
            "fa22-cs425-5501.cs.illinois.edu", "fa22-cs425-5502.cs.illinois.edu",
            "fa22-cs425-5503.cs.illinois.edu", "fa22-cs425-5504.cs.illinois.edu",
            "fa22-cs425-5505.cs.illinois.edu", "fa22-cs425-5506.cs.illinois.edu",
            "fa22-cs425-5507.cs.illinois.edu", "fa22-cs425-5508.cs.illinois.edu",
            "fa22-cs425-5509.cs.illinois.edu", "fa22-cs425-5510.cs.illinois.edu"
    };
    private String regexp;
    private String option;
    final static int serverPort = 9999;

    private static final Set<String> optionMap;
    static {
        optionMap = new HashSet<>();
        optionMap.add("-c");
        optionMap.add("-F");
        optionMap.add("-i");
        optionMap.add("-l");
        optionMap.add("-v");
        optionMap.add("-x");
        optionMap.add("-n");
        optionMap.add("-Ec");
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.startQuery(args);
    }

    public int startQuery(String[] args) {
        int totalLine = 0;
        // Check the length and validity of the arguments
        if (args.length <= 1 || args.length > 3) {
            System.out.println("invalid argument length");
            exit(1);
        } else if (args.length == 2) {
            if (!args[0].equals("grep")) {
                System.out.println("invalid arguments");
                exit(1);
            }
            option = "-n";
            regexp = args[1];
        } else {
            if (!args[0].equals("grep")) {
                System.out.println("invalid arguments");
                exit(1);
            }
            if (!optionMap.contains(args[1])) {
                System.out.println("undefined option");
                exit(1);
            }
            option = args[1];
            regexp = args[2];
        }

        try {
            // Create multiple threads where each thread handles query to one particular remote machine
            List<Future<MachineStats>> futures = execute();

            // Print matched lines from distributed log files
            printMatchedLines();

            // Print statistics, status and results from functioning machines
            totalLine = printStatistics(futures);
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.out.println("RUN TIME EXCEPTION");
        }
        return totalLine;
    }

    public List<Future<MachineStats>> execute() throws InterruptedException, ExecutionException {
        // Create thread pool to manage threads, and add callable tasks to list
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Callable<MachineStats>> callables = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            callables.add(new LogHandler(IPs[i], serverPort, regexp, option));
        }
        // Execute all callable tasks, obtain and print the results when all complete
        List<Future<MachineStats>> futures = executorService.invokeAll(callables);
        for(int i = 0; i < futures.size(); ++i){
            if (futures.get(i).get().getStatus() == 0) System.out.println("Machine " + (i + 1) +": " + "Success");
            else if (futures.get(i).get().getStatus() == 1) System.out.println("Machine " + (i + 1) +": " + "Socket Read Timeout");
            else System.out.println("Machine " + (i + 1) +": " + "Connection Failed");
        }
        executorService.shutdown();
        return futures;
    }

    public void printMatchedLines() throws IOException {
        File dir = new File(new File("").getAbsolutePath() + "/log");
        File[] logs = dir.listFiles();
        // If option -c is detected, do not print the whole matched line, but only line counts
        if ((!option.equals("-c") && !option.equals("-Ec")) && logs != null) {
            System.out.println("-----------------------------");
            BufferedReader br;
            for (File log: logs) {
                br = new BufferedReader(new FileReader(log));
                String s;
                while ((s = br.readLine()) != null) {
                    System.out.println(hostname[log.getName().charAt(7) - '0' - 1]+ ":" + s);
                }
            }
        }
    }

    public int printStatistics(List<Future<MachineStats>> futures) throws ExecutionException, InterruptedException {
        System.out.println("-----------------------------");
        int  totalLineCount = 0, currentLineCount = 0, numberOfWorkingMachine = 0;
        long maxQueryTime = 0, currentQueryTime = 0, totalQueryTime = 0;
        for (int i = 0; i < futures.size(); ++i) {
            // Ignore the failed machine
            if (futures.get(i).get().getStatus() == 0) {
                numberOfWorkingMachine++;
                currentLineCount = futures.get(i).get().getLineCount();
                currentQueryTime = futures.get(i).get().getDuration().toMillis();
                totalLineCount += currentLineCount;
                totalQueryTime += currentQueryTime;
                maxQueryTime = Math.max(maxQueryTime, currentQueryTime);
                System.out.println(hostname[i] + ": " + currentLineCount + " lines matched, " + currentQueryTime + " milliseconds query time");
            }
        }
        // Print summary of the distributed log files returned
        System.out.println("In total: " + totalLineCount + " lines matched, " + maxQueryTime + " milliseconds query time");
        System.out.println("Average query latency: " + String.format("%.3f", (double)totalQueryTime / numberOfWorkingMachine) + " milliseconds");
        return totalLineCount;
    }
}

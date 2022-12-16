import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.*;
import static utils.Constant.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class Server {
    private final Map<String, MemberStatus> membershipList = new ConcurrentHashMap<>();

    // store sockets
    private final Map<String, DatagramSocket> neighborSockets = new ConcurrentHashMap<>();
    // store host's name
    private final Map<String, InetAddress> neighborInetAddress = new ConcurrentHashMap<>();

    // every sever stores file list
    private final Map<String, List<String>> serverStoreFiles = new ConcurrentHashMap<>();
    // Record ping time
    private final Map<String, Long> timeSet = new ConcurrentHashMap<>();
    private final String HOSTNAME;
    private String id;

    private final SDFS sdfs = new SDFS();
    private Map<String, List<String>> fileMetaInfo = new ConcurrentHashMap<>();
    private final Map<String, SDFSFileInfo> storedFile = new HashMap<>();

    private Map<String, Integer> fileToLatestVersion = new HashMap<>();

    private final SDFSSender sender = new SDFSSender();
    private final Map<String, JobStatus> jobMetaInfo = new ConcurrentHashMap<>();

    private int globalJobID = 1;

    private Map<String, MachineInferenceStatus> machineToJob = new ConcurrentHashMap<>();
    private volatile int batchSizeNumber = 1;
    private volatile int batchSizeWeather = 1;
    private volatile int implicitBatchSizeNumber = 1 * TRICK;
    private volatile int implicitBatchSizeWeather = 1;

    private int numLocaJob = 0;
    private Deque<Integer> numberQueryRate = new ConcurrentLinkedDeque<>();
    private Deque<Integer> weatherQueryRate = new ConcurrentLinkedDeque<>();
    private final CommandHandler commandHandler = new CommandHandler();


    private volatile boolean firstInference = true;



    public Server() throws IOException {
        File file = new File(new File("").getAbsolutePath() + "/utils/master.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader buffer = new BufferedReader(fileReader);
        INTRODUCER = buffer.readLine();
        MASTER = INTRODUCER;
        System.out.println("========" + MASTER + "========");
        fileReader.close();
        buffer.close();
        HOSTNAME = InetAddress.getLocalHost().getHostName();
        id = System.currentTimeMillis() + HOSTNAME.substring(13, 15);
        // Initialize membership list
        for (String host : GROUP) {
            membershipList.put(host, new MemberStatus(false, ""));
        }
        // Mark process itself as alive
        if (INTRODUCER.equals(HOSTNAME)) {
            membershipList.put(HOSTNAME, new MemberStatus(true, id));
        }
        // Initialize UDP sockets
        String[] neighbors = Neighbors.get(HOSTNAME);
        for (int i = 0; i < 4; i++) {
            DatagramSocket neighbor = new DatagramSocket();
            neighborSockets.put(neighbors[i], neighbor);
            neighborInetAddress.put(neighbors[i], InetAddress.getByName(neighbors[i]));
        }
        // Initialize machineToJob
        for (String machine: GROUP) {
            machineToJob.put(machine, new MachineInferenceStatus(null, null, null, -1, 0, true));
        }
    }


    /*
     * Base class to send message
     */
    private abstract class MemberSend implements Runnable {
        // Type of the message
        protected String type;

        @Override
        public void run() {
            try {
                DatagramSocket socket = new DatagramSocket();
                InetAddress destination = getDes(INTRODUCER);
                sendMessage(socket, destination);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        protected void sendMessage(DatagramSocket socket, InetAddress destination) throws IOException {
            JSONObject msg = generateMsg();
            byte[] buffer = new byte[1024];
            buffer = msg.toString().getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destination, PORT);
            socket.send(packet);
        }

        protected JSONObject generateMsg() {
            JSONObject msg = new JSONObject();
            msg.put(TYPE, type);
            msg.put(HOST, HOSTNAME);
            msg.put(ID, id);
            msg.put(SENDTime, System.currentTimeMillis());
            return msg;
        }

        public InetAddress getDes(String des) throws UnknownHostException {
            return neighborInetAddress.get(des);
        }
    }

    /*
     * MemberLeave runnable class is invoked when any member wants to leave the group voluntarily
     */
    private class MemberLeave extends MemberSend {
        public MemberLeave() {
            System.out.println("Leave thread is created");
            this.type = LEAVE;
        }
        @Override
        public void run() {
            try {
                String oldId = membershipList.get(HOSTNAME).getId();
                membershipList.replace(HOSTNAME, new MemberStatus(false, oldId));
                for (String neighbor: Neighbors.get(HOSTNAME)) {
                    if (membershipList.get(neighbor).getAlive()) {
                        DatagramSocket socket = neighborSockets.get(neighbor);
                        sendMessage(socket, getDes(neighbor));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /*
     * MemberJoin runnable class is invoked when any member wants to join the group
     */
    private class MemberJoin extends MemberSend {
        public MemberJoin() {
            System.out.println("Join thread is created");
            this.type = JOIN;
        }
        @Override
        public void run() {
            System.out.println("join thread  " + type);
            super.run();
        }

        @Override
        public InetAddress getDes(String des) throws UnknownHostException {
            return InetAddress.getByName(INTRODUCER);
        }
    }

    /*
     * Sender class handles sending PING message
     */
    class Sender extends MemberSend {

        public Sender() {
            System.out.println("sender thread is created");
            this.type = PING;
        }
        @Override
        public void run() {
            try {
                while (true) {
                    // Allow alive process to send PING message
                    if (membershipList.get(HOSTNAME).getAlive()) {
                        for (String neighbor: Neighbors.get(HOSTNAME)) {
                            // Send PING message only to alive neighbors
                            if (membershipList.get(neighbor).getAlive()) {
                                Random random = new Random();
                                DatagramSocket socket = neighborSockets.get(neighbor);
                                InetAddress destination = getDes(neighbor);
                                // Record the ping time for every neighbors
                                if (!timeSet.containsKey(neighbor)) {
                                    timeSet.put(neighbor, System.currentTimeMillis());
                                }
                                sendMessage(socket, destination);
                            }
                        }
                    }
                    // Send PING message every 1 second
                    Thread.sleep(1000);
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected JSONObject generateMsg() {
            JSONObject msg = new JSONObject();
            msg.put(TYPE, type);
            msg.put(HOST, HOSTNAME);
            msg.put(SENDTime, System.currentTimeMillis());
            return msg;
        }
    }

    /*
     * Receiver class handles the reception of all types of message
     */
    class Receiver implements Runnable {
        DatagramSocket socket = new DatagramSocket(PORT);

        Receiver() throws SocketException {
        }

        @Override
        public void run() {
            while (true) {
                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String receive = new String(packet.getData(), packet.getOffset(),
                            packet.getLength(), StandardCharsets.UTF_8);
                    JSONObject msg = new JSONObject(receive);
                    if (msg.has(TYPE)) {
                        String type = (String)msg.get(TYPE);
                        switch (type) {
                            case PING: {
                                if (msg.get(HOST).equals(HOSTNAME)) break;
                                if (membershipList.get(HOSTNAME).getAlive()) sendACK(msg);
                                break;
                            }

                            case JOIN: {
                                String joinHostname = (String) msg.get(HOST);
                                String joinId = (String)msg.get(ID);
                                // Update membership list to reflect join of new process
                                membershipList.replace(joinHostname, new MemberStatus(true, joinId));
                                // Record machine JOIN events
                                new Thread(new LogRecorder((String)msg.get(TYPE), (String)msg.get(HOST), false, "")).start();
                                // Introducer needs to carry out more actions when receiving JOIN message
                                if (HOSTNAME.equals(INTRODUCER)) {
                                    if (MASTER_BACKUP.contains(joinHostname)) {
                                        JSONObject json = new JSONObject();
                                        json.put(TYPE, BACK_UP_JOIN);
                                        JSONArray fileMetaInfoKey = new JSONArray();
                                        JSONArray fileMetaInfoValue = new JSONArray();
                                        for (String key : fileMetaInfo.keySet()) {
                                            fileMetaInfoKey.put(key);
                                            JSONArray jsonArray = new JSONArray();
                                            for (String s : fileMetaInfo.get(key)) {
                                                jsonArray.put(s);
                                            }
                                            fileMetaInfoValue.put(jsonArray);
                                        }
                                        json.put(FILE_META_INFO_KEY, fileMetaInfoKey);
                                        json.put(FILE_META_INFO_VALUE, fileMetaInfoValue);
                                        JSONArray fileVersionKey = new JSONArray();
                                        JSONArray fileVersionValue = new JSONArray();
                                        for (String key : fileToLatestVersion.keySet()) {
                                            fileVersionKey.put(key);
                                            fileVersionValue.put(fileToLatestVersion.get(key));
                                        }
                                        json.put(FILE_VERSIONS_KEY, fileVersionKey);
                                        json.put(FILE_VERSIONS_VALUE, fileVersionValue);
                                        sender.sendRequest(json, joinHostname, BACKUPPORT);
                                    }
                                    // Send introducer's membership list to the joining process
                                    JSONObject json = new JSONObject();
                                    json.put(TYPE, MEMBERSHIPLIST);
                                    json.put(SENDTime, System.currentTimeMillis());
                                    JSONArray jsonArray = new JSONArray();
                                    for (Map.Entry<String, MemberStatus> entry : membershipList.entrySet()) {
                                        jsonArray.put(entry.getValue().getId());
                                    }
                                    json.put(IDLIST, jsonArray);
                                    for (Map.Entry<String, MemberStatus> entry : membershipList.entrySet()) {
                                        json.put(entry.getKey(), entry.getValue().getAlive());
                                    }
                                    buffer = json.toString().getBytes(StandardCharsets.UTF_8);
                                    packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(joinHostname), PORT);
                                    socket.send(packet);
                                    // Multicast JOIN messaSocket s=new Socket("localhost",6666);ge to all members in the group
                                    for (String member : GROUP) {
                                        if (!member.equals(INTRODUCER) && !member.equals(joinHostname) && membershipList.get(member).getAlive()) {
                                            JSONObject multicast = new JSONObject();
                                            multicast.put(TYPE, JOIN);
                                            multicast.put(HOST, joinHostname);
                                            multicast.put(ID, joinId);
                                            multicast.put(SENDTime, System.currentTimeMillis());
                                            buffer = multicast.toString().getBytes(StandardCharsets.UTF_8);
                                            packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(member), PORT);
                                            socket.send(packet);
                                        }
                                    }
                                }
                                break;
                            }

                            case MEMBERSHIPLIST: {
                                JSONArray idList = (JSONArray) msg.get(IDLIST);
                                int counter = 0;
                                // When receiving membership list from introducer, replace the old membership list with the new one
                                for (Map.Entry<String, MemberStatus> entry : membershipList.entrySet()) {
                                    membershipList.replace(entry.getKey(), new MemberStatus((Boolean)msg.get(entry.getKey()), idList.getString(counter)));
                                    counter++;
                                }
                                break;
                            }

                            case LEAVE: {
                                disseminateMsg(LEAVE, msg);
                                break;
                            }

                            case FAIL: {
                                disseminateMsg(FAIL, msg);
                                break;
                            }

                            case ACK: {
                                receiveACK(msg);
                                break;
                            }

                            default: {
                                System.out.println("undefined message type");
                            }
                        }
                    } else {
                        System.out.println("receive a message without type");
                    }
                    socket.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        private void disseminateMsg(String type, JSONObject msg) throws IOException {

            // Leave / fail, the hostname in msg is the leave/fail process'sJSONObject object=JSONObject.fromObject(str);
            String leaveHostname = (String) msg.get(HOST);
            boolean needCopied = membershipList.get(leaveHostname).getAlive();
            boolean needReassignJob = membershipList.get(leaveHostname).getAlive();
            List<String> filesInFailedMachine = null;
            if (serverStoreFiles.containsKey(leaveHostname)) {
                filesInFailedMachine = serverStoreFiles.get(leaveHostname);
            }

            //when fail or leave, needs to delete it from fileMetaInfo
            if (MASTER_BACKUP.contains(HOSTNAME) && serverStoreFiles.containsKey(leaveHostname)) {
                List<String> files = serverStoreFiles.get(leaveHostname);
                System.out.println(files.toString() + "\t MASTER_BACKUP.contains(HOSTNAME)");
                serverStoreFiles.remove(leaveHostname);
                for (String file : files) {
                    fileMetaInfo.get(file).remove(leaveHostname);
                }
            }

            // Only disseminate the message if leave/fail process is alive in current membership list to avoid redundant message
            if (membershipList.get(leaveHostname).getAlive()) {
                // Record machine LEAVE/FAIL events
                new Thread(new LogRecorder((String)msg.get(TYPE), (String)msg.get(HOST), false, "")).start();
                String oldId = membershipList.get(leaveHostname).getId();
                membershipList.replace(leaveHostname, new MemberStatus(false, oldId));
                // Get my own neighbors
                for (String neighbor : Neighbors.get(HOSTNAME)) {
                    // Disseminate message if my neighbor is alive
                    if (membershipList.get(neighbor).getAlive()) {
                        JSONObject json = new JSONObject();
                        json.put(TYPE, type);
                        json.put(HOST, leaveHostname);
                        json.put(SENDTime, System.currentTimeMillis());
                        byte[] buffer = json.toString().getBytes(StandardCharsets.UTF_8);
                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighborInetAddress.get(neighbor), PORT);
                        neighborSockets.get(neighbor).send(packet);
                    }
                }
            }
            if (leaveHostname.equals(MASTER)) {
                System.out.println("master fails");
                for (String host : GROUP) {
                    if (membershipList.get(host).getAlive()) {
                        INTRODUCER = host;
                        MASTER = host;
                        System.out.println(MASTER + " to be the leader");
                        if (MASTER.equals(HOSTNAME)) {
                            for (String node : GROUP) {
                                if (node.equals(HOSTNAME)) {
                                    File file = new File(new File("").getAbsolutePath() + "/utils/master.txt");
                                    FileWriter fileWriter = new FileWriter(file);
                                    fileWriter.write("");
                                    fileWriter.write(HOSTNAME);
                                    fileWriter.flush();
                                    fileWriter.close();
                                } else {
                                    JSONObject json = new JSONObject();
                                    json.put(NEW_MASTER, HOSTNAME);
                                    byte[] buffer = json.toString().getBytes(StandardCharsets.UTF_8);
                                    DatagramSocket socket = new DatagramSocket();
                                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(node), INDICATOR);
                                    socket.send(packet);
                                }

                            }
                        }
                        break;
                    }
                }
            }

            // Copy files originally stored in failed machine to another live machine
            if (HOSTNAME.equals(MASTER) && needCopied) {
                System.out.println("=======Need to send copy msg========");
                if (filesInFailedMachine != null && filesInFailedMachine.size() > 0) {
                    System.out.println("=======Contain Files in Failed Machine========");
                    List<String> fileNames = new ArrayList<>();
                    List<String> fileContacts = new ArrayList<>();
                    List<Integer> fileVersions = new ArrayList<>();
                    for (String file: filesInFailedMachine) {
                        System.out.println("=======Failed Machine contains " + file + "========");
                        fileNames.add(file);
                        fileContacts.add(fileMetaInfo.get(file).get(0));
                        fileVersions.add(fileToLatestVersion.get(file));
                    }
                    new Thread(new CopyFileBetweenMachine(leaveHostname, fileNames, fileContacts, fileVersions)).start();
                }
            }

        }

        private void sendACK(JSONObject msg) throws IOException {
            String fromHost = (String) msg.get(HOST);
            JSONObject returnMsg = new JSONObject();
            returnMsg.put(TYPE, ACK);
            returnMsg.put(HOST, HOSTNAME);
            returnMsg.put(SENDTime, msg.get(SENDTime));
            byte[] buffer = returnMsg.toString().getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length,
                    neighborInetAddress.get(fromHost), PORT);
            neighborSockets.get(fromHost).send(packet);
        }

        private void receiveACK(JSONObject msg) throws IOException {
            String fromNeighbor = (String) msg.get(HOST);
            String processId = membershipList.get(fromNeighbor).getId();
            // Update membership list when receive ACK message
            if (!membershipList.get(fromNeighbor).getAlive()) {
                membershipList.replace(fromNeighbor, new MemberStatus(true, processId));
            }
            long curTime = System.currentTimeMillis();
            long sendTime = msg.getLong(SENDTime);
            timeSet.remove(fromNeighbor);
            // If the interval is too long, then assume the node is failed
            if (sendTime - curTime > TIMEOUT) {
                JSONObject leaveMsg = new JSONObject();
                leaveMsg.put(HOST, fromNeighbor);
                disseminateMsg(FAIL, leaveMsg);
            }
        }
    }

    /*
     * TimeoutChecker class periodically checks whether timeout occurs in membership list to detect process failure
     */
    private class TimeoutChecker implements Runnable {

        @Override
        public void run() {
            while (true) {
                Iterator<Map.Entry<String, Long>> it = timeSet.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Long> entry = it.next();
                    long curTime = System.currentTimeMillis();
                    if (curTime - entry.getValue() > TIMEOUT) {
                        //System.out.println("================AFTER SENDING " + count + "times ping, fail occurs====================");
                        it.remove();
                        String failNeighbor = entry.getKey();
                        String oldId = membershipList.get(failNeighbor).getId();
                        // If any member times out in membership list, disseminate failure to alive neighbors
                        for (String neighbor : Neighbors.get(HOSTNAME)) {
                            if (membershipList.get(neighbor).getAlive()) {
                                JSONObject json = new JSONObject();
                                json.put(TYPE, FAIL);
                                json.put(HOST, failNeighbor);
                                json.put(SENDTime, System.currentTimeMillis());
                                byte[] buffer = json.toString().getBytes(StandardCharsets.UTF_8);
                                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighborInetAddress.get(neighbor), PORT);
                                try {
                                    neighborSockets.get(neighbor).send(packet);
                                } catch (IOException e) {
                                    System.out.println("Fail detected error");
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(TIMEOUT / 4);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /*
     * LogRecorder class is used to log events
     */
    class LogRecorder implements Runnable {
        private String event;
        private String hostname;

        private boolean newLog = false;
        private String file;


        public LogRecorder(String event, String hostname, boolean newLog, String file) {
            this.hostname = hostname;
            this.event = event;
            this.file = file;
            this.newLog = newLog;
        }

        @Override
        public void run() {
            try {
                Files.createDirectories(Paths.get(new File("").getAbsolutePath() + "/log/"));
                System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
                String fileName = new File(new File("").getAbsolutePath()) + "/log/" + "machine.";
                if (HOSTNAME.charAt(13) == '0') fileName += HOSTNAME.charAt(14);
                else fileName += HOSTNAME.substring(13, 15);
                fileName += ".log";
                FileHandler fileHandler = new FileHandler(fileName, true);
                Logger logger = Logger.getLogger(LogRecorder.class.getName());
                fileHandler.setFormatter(new SimpleFormatter() {
                    private static final String format = "[%1$tF %1$tT] [%2$-7s] %3$s %n";
                    @Override
                    public synchronized String format(LogRecord logRecord)
                    {
                        return String.format(format, new Date(logRecord.getMillis()), logRecord.getLevel().getLocalizedName(), logRecord.getMessage());
                    }
                });
                logger.addHandler(fileHandler);
                if (!newLog) {
                    logger.info(hostname + " " + event);
                } else {
                    if (event.equals(GET)) {
                        logger.info(hostname + " " + "GET " + file);
                    } else if (event.equals(PUT)) {
                        logger.info(hostname + " " + "PUT " +file);
                    } else if (event.equals(DELETE)) {
                        logger.info(hostname + " " + "DELETE " +file);
                    } else if (event.equals(STORE)) {
                        logger.info(hostname + " " + "STORE");
                    } else if (event.equals(LS)) {
                        logger.info(hostname + " " + "LS " +file);
                    } else if (event.equals(GET_VERSION)) {
                        logger.info(hostname + " " + "GET-VERSIONS " +file);
                    }
                }
                fileHandler.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }



/*
 * MP3 SDFS
 */

    public class Responder implements Runnable {

        ServerSocket serverSocket;

        @Override
        public void run() {
            try {
                serverSocket = new ServerSocket(RESPONDERRPORT);
                while (true) {
                    new Thread(new ResponderHandler(serverSocket.accept())).start();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class ResponderHandler implements Runnable {

        private Socket socket;
        public ResponderHandler(Socket socket) {
            this.socket = socket;
        }


        @Override
        public void run() {
            InputStreamReader isr;
            OutputStreamWriter osw;
            BufferedReader bufferedReader = null;
            BufferedWriter bufferedWriter = null;
            DataOutputStream out = null;
            DataInputStream dis = null;

            try {
                isr = new InputStreamReader(socket.getInputStream());
                bufferedReader = new BufferedReader(isr);
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);
                dis = new DataInputStream(socket.getInputStream());
                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                byte[] buffer = new byte[4*1024];
                int read = dataInputStream.read(buffer, 0, 1024);
                //System.out.println("read:" + read);
                JSONObject receivedJSON = new JSONObject(new String(buffer,0, 1024, StandardCharsets.UTF_8));
//                System.out.println("responder handler:\t" + receivedJSON);
                if (receivedJSON.has(TYPE)) {
                    String type = receivedJSON.getString(TYPE);
                    switch (type) {
                        case FILEUPDATETIME: {
                            JSONObject json = new JSONObject();
                            json.put(TYPE, FILEUPDATETIME);
                            if (!storedFile.containsKey(receivedJSON.getString(FILENAME))) {
                                json.put(GET_FILE_UPDATE_SUCCESS, false);
                                bufferedWriter.write(json + "\n");
                                bufferedWriter.flush();
                                break;
                            }
                            json.put(FILEUPDATE, storedFile.get(receivedJSON.getString(FILENAME)).getLastUpdatedTime().toString());
                            json.put(HOST, InetAddress.getLocalHost().getHostName());
                            json.put(GET_FILE_UPDATE_SUCCESS, true);
                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            break;
                        }
                        case DELETE: {
                            String sdfsFile = receivedJSON.getString(SDFSFILE);
                            storedFile.remove(sdfsFile);
                            JSONObject result = new JSONObject();
                            result.put(TYPE, DELETE_RESULT);
                            result.put(DELETE_RESULT, true);
                            bufferedWriter.write(result + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case GETFILE: {
                            // Get the requested file name
                            String SDFSFileName = receivedJSON.getString(FILENAME);
                            // Transfer latest file to requester
                            int currentNumVersion = storedFile.get(SDFSFileName).getLatestVersion();
                            dis = new DataInputStream(new BufferedInputStream(new FileInputStream(new File("").getAbsolutePath() + "/SDFS_files/" + SDFSFileName + "_" + currentNumVersion)));
                            out = new DataOutputStream(socket.getOutputStream());
                            int current =0;
                            while((current = dis.read(buffer))!=-1){
                                out.write(buffer, 0, current);
                            }
                            out.flush();
                            break;
                        }

                        case GET_VERSION: {
                            // Get the requested file name
                            String SDFSFileName = receivedJSON.getString(FILENAME);
                            int versionNum = receivedJSON.getInt(VERSION_NUM);
                            String versionedFileName = new File("").getAbsolutePath() + "/SDFS_files/" + SDFSFileName + "_" + versionNum;
                            File file = new File(versionedFileName);
                            if (receivedJSON.keySet().contains(ACTION)) {
                                JSONObject copyJson = new JSONObject();
                                if (file.isDirectory()) {
                                    System.out.println("GET_VERSION in DICT");
                                    file = tryZipDict(versionedFileName);
                                    copyJson.put(FILE_TYPE, DICT);
                                } else {
                                    copyJson.put(FILE_TYPE, FILE);
                                }
                                int idx = 0;
                                for (byte b : copyJson.toString().getBytes()) {
                                    buffer[idx] = b;
                                    idx++;
                                }
                                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                                dataOutputStream.write(buffer, 0, 1024);
                                dataOutputStream.flush();
                            }
                            // Transfer file with specified version to requester
                            dis = new DataInputStream(new BufferedInputStream(new FileInputStream(new File("").getAbsolutePath() + "/SDFS_files/" + file.getName())));
                            out = new DataOutputStream(socket.getOutputStream());
                            int current = 0;
                            while((current = dis.read(buffer))!=-1){
                                out.write(buffer, 0, current);
                            }
                            out.flush();
                            break;
                        }

                        case PUTFILE: {
                            String sdfsFile = receivedJSON.getString(SDFSFILE);
                            int latestVersion = receivedJSON.getInt(LATEST_VERSION);
                            String fileName;
                            String fileType = receivedJSON.getString(FILE_TYPE);
                            // Take action according to whether the file has already existed
                            if (!storedFile.containsKey(sdfsFile)) {
                                fileName = sdfsFile + "_" + (latestVersion);
                                storedFile.put(sdfsFile, new SDFSFileInfo(new Timestamp(System.currentTimeMillis()), latestVersion));
                            } else {
                                fileName = sdfsFile + "_" + latestVersion;
                                storedFile.get(sdfsFile).setLastUpdatedTime(new Timestamp(System.currentTimeMillis()));
                                storedFile.get(sdfsFile).setLatestVersion(latestVersion);
                            }
                            if (fileType.equals(DICT)) {
                                fileName += ".zip";
                            }
                            String path = new File("").getAbsolutePath() + "/SDFS_files/" + fileName;
                            FileOutputStream fileOutputStream = new FileOutputStream(path);
                            int bytes;
                            long size = receivedJSON.getLong(FILE_SIZE);
                            long reqTime = receivedJSON.getLong(REQ_TIME);
                            System.out.println("file size:" + size);
                            while (size > 0 && (bytes = dis.read(buffer,0,(int)Math.min(size, 4096))) != -1) {
                                fileOutputStream.write(buffer,0,bytes);
                                size -= bytes;
                            }
                            fileOutputStream.close();
                            System.out.println("Put use " + (System.currentTimeMillis() - reqTime) + "ms");
                            if (fileType.equals(DICT)) {
                                String dest = new File("").getAbsolutePath() + "/SDFS_files";
                                tryUnZip(path, dest, sdfsFile + "_" + latestVersion);
                            }
                            break;
                        }

                        case COPY_FILES: {
                            JSONArray fileNames = receivedJSON.getJSONArray(FILES_TO_BE_COPIED);
                            JSONArray fileContacts = receivedJSON.getJSONArray(FILE_CONTACT);
                            JSONArray fileVersions = receivedJSON.getJSONArray(FILES_TO_LATEST_VERSION);
                            List<Integer> nonExist = new ArrayList<>();
                            for (int i = 0; i < fileNames.length(); ++i) {
                                if (!storedFile.containsKey(fileNames.getString(i))) {
                                    nonExist.add(i);
                                }
                            }
                            boolean isSuccess = true;
                            // Launch one thread for each file
                            if (nonExist.size() > 0) {
                                ExecutorService executorService = Executors.newFixedThreadPool(nonExist.size());
                                List<Callable<Boolean>> callables = new ArrayList<>();
                                for (int i: nonExist) {
                                    callables.add(new CopyFile(fileNames.getString(i), fileContacts.getString(i), fileVersions.getInt(i)));
                                }
                                List<Future<Boolean>> futures = executorService.invokeAll(callables);
                                for(int i = 0; i < futures.size(); ++i){
                                    if (!futures.get(i).get()) {
                                        isSuccess = false;
                                        break;
                                    }
                                }
                            }

                            // Reply ACK if successfully copy files
                            JSONObject json = new JSONObject();
                            json.put(TYPE, COPY_FILE_REPLY);
                            if (isSuccess) {
                                json.put(COPY_FILE_SUCCESS, true);
                            } else {
                                json.put(COPY_FILE_SUCCESS, false);
                            }
                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        // Do inference task
                        case INFERENCE: {

                            numLocaJob++;
                            //System.out.println("=======num local job: " + numLocaJob + "========");

                            String inputFile = receivedJSON.getString(FILENAME);
                            String model = receivedJSON.getString(MODEL);
                            String jobID = receivedJSON.getString(JOB_ID);
                            int batchSize = receivedJSON.getInt(BATCH_SIZE);
                            int batchNum = receivedJSON.getInt(BATCH_NUMBER);
                            ExecutorService executorService = Executors.newFixedThreadPool(1);
                            List<Callable<List<String>>> callables = new ArrayList<>();
                            callables.add(new InferenceTask(inputFile, jobID, model, batchNum, batchSize));
                            List<Future<List<String>>> futures = executorService.invokeAll(callables);
                            // Reply ACK if successfully complete inference task
                            JSONObject json = new JSONObject();
                            json.put(TYPE, INFERENCE_REPLY);


                            numLocaJob--;


                            if (futures.size() == 1 && futures.get(0).get().size() > 0) {
                                System.out.println("========Inference Succeed (" + model + ") " + batchNum + "========");
                                json.put(INFERENCE_SUCCESS, true);
                                JSONArray jsonArray = new JSONArray();
                                for (String s: futures.get(0).get()) {
                                    jsonArray.put(s);
                                }
                                json.put(INFERENCE_ARRAY_REPLY, jsonArray);
                            } else {
                                json.put(INFERENCE_SUCCESS, false);
                            }

                            // Reply if the inference is successful
                            json.put(BATCH_START_TIME, receivedJSON.getLong(BATCH_START_TIME));
                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case DIR_EXIST_REQUEST: {
                            String dir = receivedJSON.getString(FILENAME);
                            File folder = new File(new File("").getAbsolutePath() + "/SDFS_files/" + dir + "_1");
                            JSONObject result = new JSONObject();
                            result.put(IS_DIR_EXIST, folder.exists());
                            bufferedWriter.write(result + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case STOP_PY: {
                            String execution = "./stopPy.sh";
                            System.out.println(execution);
                            Runtime.getRuntime().exec(execution);
                            break;
                        }

                        case C3_SETUP: {
                            String model = receivedJSON.getString(MODEL);
                            int batchSize = receivedJSON.getInt(BATCH_SIZE);
                            if (model.equals("number")) {
                                batchSizeNumber = batchSize;
                            } else if (model.equals("weather")) {
                                batchSizeWeather = batchSize;
                                implicitBatchSizeNumber = receivedJSON.getInt(IMPLICIT_BATCH_SIZE_NUMBER);
                            }
                            JSONObject result = new JSONObject();
                            result.put(C3_REPLY, true);
                            bufferedWriter.write(result + "\n");
                            bufferedWriter.flush();
                            break;
                        }
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (bufferedWriter != null) bufferedWriter.close();
                    if (bufferedReader != null) bufferedReader.close();
                    socket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }

        private File tryZipDict(String path) {
            File zipFile = new File(path + ".zip");
            File tobeZipped = new File(path);
            if (zipFile.exists()) {
                return zipFile;
            }
            try {
                InputStream input;
                ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));

                File[] files = tobeZipped.listFiles();
                for(int i = 0; i < files.length; ++i){
                    input = new FileInputStream(files[i]);
                    zipOut.putNextEntry(new ZipEntry(tobeZipped.getName() + File.separator + files[i].getName()));
                    int temp = 0;
                    while((temp = input.read()) != -1){
                        zipOut.write(temp);
                    }
                    input.close();
                }
                zipOut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new File(path + ".zip");
        }
    }

    private void tryUnZip(String zipPath, String dest, String dirName) throws IOException {
        System.out.println("zipPath:" + zipPath);
        System.out.println("dest:" + dest);
        System.out.println("dirName:" + dirName);
        File file = new File(zipPath);
        if (!file.exists()) {
            throw new RuntimeException(zipPath + " no file exists");
        }
        ZipFile zf = new ZipFile(file);
        Enumeration entries = zf.entries();
        ZipEntry entry = null;
        while (entries.hasMoreElements()) {
            entry = (ZipEntry) entries.nextElement();
            if (entry.isDirectory()) {
                String dirPath = dest + File.separator + dirName + File.separator + entry.getName().split("/")[1];
                System.out.println("");
                File dir = new File(dirPath);
                dir.mkdirs();
            } else {
                String filePath = dest + File.separator + dirName + File.separator + entry.getName().split("/")[1];
                File f = new File(filePath);
                if (!f.exists()) {
                    String dirs = f.getParent();
                    File parentDir = new File(dirs);
                    parentDir.mkdirs();
                }
                f.createNewFile();
                InputStream is = zf.getInputStream(entry);
                FileOutputStream fos = new FileOutputStream(f);
                int count;
                byte[] buf = new byte[8192];
                while ((count = is.read(buf)) != -1) {
                    fos.write(buf, 0, count);
                }
                is.close();
                fos.close();
            }
        }
        File zipFile = new File(zipPath);
        zipFile.delete();
    }

    public class BackUpHandler implements Runnable {
        ServerSocket serverSocket = new ServerSocket(BACKUPPORT);

        public BackUpHandler() throws IOException {
        }

        @Override
        public void run() {
            InputStreamReader isr;
            BufferedReader bufferedReader;
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    isr = new InputStreamReader(socket.getInputStream());
                    bufferedReader = new BufferedReader(isr);
                    String s = bufferedReader.readLine();
                    JSONObject receivedJSON = new JSONObject(s);
                    if (receivedJSON.has(TYPE)) {
                        String type = (String) receivedJSON.get(TYPE);
                        switch (type) {
                            case BACK_UP_JOIN: {
                                JSONArray fileMetaInfoKey = receivedJSON.getJSONArray(FILE_META_INFO_KEY);
                                JSONArray fileMetaInfoValue = receivedJSON.getJSONArray(FILE_META_INFO_VALUE);
                                JSONArray fileVersionKey = receivedJSON.getJSONArray(FILE_VERSIONS_KEY);
                                JSONArray fileVersionValue = receivedJSON.getJSONArray(FILE_VERSIONS_VALUE);
                                int fileMetaSize = fileMetaInfoKey.length();
                                for (int i = 0; i < fileMetaSize; i++) {
                                    List<String> temp = new ArrayList<>();
                                    JSONArray list = fileMetaInfoValue.getJSONArray(i);
                                    int size = list.length();
                                    for (int j = 0; j < size; j++) {
                                        temp.add((String) list.get(j));
                                    }
                                    fileMetaInfo.put(fileMetaInfoKey.getString(i), temp);
                                }
                                int fileVersionSize = fileVersionKey.length();
                                for (int i = 0; i < fileVersionSize; i++) {
                                    fileToLatestVersion.put(fileVersionKey.getString(i), fileVersionValue.getInt(i));
                                }
                                OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
                                BufferedWriter bufferedWriter = new BufferedWriter(osw);
                                JSONObject request = new JSONObject();
                                bufferedWriter.write(request.toString() + "\n");
                                bufferedWriter.flush();
                                osw.close();
                                bufferedWriter.close();
                                break;
                            }

                            case BACK_UP_PUT: {
                                String sdfsFile = receivedJSON.getString(SDFSFILE);
                                JSONArray hosts = (JSONArray) receivedJSON.get(STOREHOSTS);
                                int latestVersion = receivedJSON.getInt(LATEST_VERSION);
                                fileToLatestVersion.put(sdfsFile, latestVersion);
                                int size = hosts.length();
                                List<String> fileHolders = fileMetaInfo.getOrDefault(sdfsFile, new ArrayList<>());
                                for (int i = 0; i < size; i++) {
                                    String server = (String) hosts.get(i);
                                    if (!fileHolders.contains(server)) {
                                        fileHolders.add(server);
                                    }
                                    List<String> files = serverStoreFiles.getOrDefault(server, new ArrayList<>());
                                    if (!files.contains(sdfsFile)) {
                                        files.add(sdfsFile);
                                        serverStoreFiles.put(server, files);
                                    }
                                }
                                fileMetaInfo.put(sdfsFile, fileHolders);
                                OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
                                BufferedWriter bufferedWriter = new BufferedWriter(osw);
                                JSONObject request = new JSONObject();
                                bufferedWriter.write(request.toString() + "\n");
                                bufferedWriter.flush();
                                osw.close();
                                bufferedWriter.close();
                                break;
                            }

                            case BACK_UP_DELETE: {
                                String sdfsFile = receivedJSON.getString(SDFSFILE);
                                List<String> servers = fileMetaInfo.get(sdfsFile);
                                System.out.println("====BACK UP Receive DELETE=====");
                                fileMetaInfo.remove(sdfsFile);
                                fileToLatestVersion.remove(sdfsFile);
                                for (String server : servers) {
                                    if (serverStoreFiles.containsKey(server)) {
                                        serverStoreFiles.get(server).remove(sdfsFile);
                                    }
                                }
                                OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
                                BufferedWriter bufferedWriter = new BufferedWriter(osw);
                                JSONObject request = new JSONObject();
                                bufferedWriter.write(request.toString() + "\n");
                                bufferedWriter.flush();
                                osw.close();
                                bufferedWriter.close();
                                break;
                            }
                            case BACK_UP_INFERENCE: {
                                String jobID = receivedJSON.getString(JOB_ID);
                                int batchIndex = receivedJSON.getInt(BATCH_NUMBER);
                                String inputFile = receivedJSON.getString(FILENAME);
                                String model = receivedJSON.getString(MODEL);
                                int batchSize = receivedJSON.getInt(BATCH_SIZE);
                                implicitBatchSizeNumber = receivedJSON.getInt(IMPLICIT_BATCH_SIZE_NUMBER);
                                batchSizeWeather = receivedJSON.getInt("batchWeather");
                                batchSizeNumber = receivedJSON.getInt("batchNum");
                                int totalBatchSize = 0;
                                String machine = receivedJSON.getString(HOST);
                                long time = receivedJSON.getLong(BATCH_TIME);
                                JSONArray writeResult = receivedJSON.getJSONArray(INFERENCE_ARRAY_REPLY);
                                if (!jobMetaInfo.containsKey(jobID)) {
                                    //System.out.println("Back up does not have " + jobID + " add it");
                                    jobMetaInfo.put(jobID, new JobStatus(inputFile, model, batchSize, totalBatchSize));
                                }
                                if (!jobMetaInfo.get(jobID).getProcessed().contains(batchIndex)) {
                                    File file = new File(new File("").getAbsolutePath() + "/local_files/" + jobID);
                                    FileOutputStream fileOutputStream = new FileOutputStream(file,true);
                                    OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, "UTF-8");
                                    for (int i = 0; i != writeResult.length(); ++i) {
                                        outputStreamWriter.write(writeResult.getString(i) + "\n");
                                    }
                                    outputStreamWriter.close();
                                    jobMetaInfo.get(jobID).getProcessed().add(batchIndex);
                                    jobMetaInfo.get(jobID).getQueryProcessingTimes().add(time);
                                }

                                if (model.equals("number")) {
                                    if (numberQueryRate.size() < 6) {
                                        numberQueryRate.addLast(jobMetaInfo.get(jobID).getProcessed().size());
                                    } else if (numberQueryRate.size() == 6) {
                                        numberQueryRate.removeFirst();
                                        numberQueryRate.addLast(jobMetaInfo.get(jobID).getProcessed().size());
                                    }
                                } else {
                                    if (weatherQueryRate.size() < 6) {
                                        weatherQueryRate.addLast(jobMetaInfo.get(jobID).getProcessed().size());
                                    } else if (weatherQueryRate.size() == 6) {
                                        weatherQueryRate.removeFirst();
                                        weatherQueryRate.addLast(jobMetaInfo.get(jobID).getProcessed().size());
                                    }
                                }
                                JSONObject jsonObject = new JSONObject();
                                jsonObject.put("reply", "reply");
                                OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
                                BufferedWriter bufferedWriter = new BufferedWriter(osw);
                                bufferedWriter.write(jsonObject.toString() + "\n");
                                bufferedWriter.flush();
                                osw.close();
                                bufferedWriter.close();
                                break;
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public class CopyFile implements Callable<Boolean> {

        String fileName;
        String destination;
        int latestVersion;

        public CopyFile(String fileName, String destination, int latestVersion) {
            this.fileName = fileName;
            this.destination = destination;
            this.latestVersion = latestVersion;
        }

        @Override
        public Boolean call() throws Exception {

            Socket socket;
            DataInputStream dis;
            DataOutputStream localFileDos = null;
            OutputStreamWriter osw;
            BufferedWriter bufferedWriter;

            for (int i = 1; i <= latestVersion; ++i) {
                socket = new Socket(destination, RESPONDERRPORT);
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);

                JSONObject getVersionsRequest = new JSONObject();
                getVersionsRequest.put(TYPE, GET_VERSION);
                getVersionsRequest.put(ACTION, COPY_FILES);
                getVersionsRequest.put(FILENAME, fileName);
                getVersionsRequest.put(CLIENT, HOSTNAME);
                getVersionsRequest.put(VERSION_NUM, i);
                bufferedWriter.write(getVersionsRequest.toString() + "\n");
                bufferedWriter.flush();

                dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                int bufferedSize = 1024 * 10;
                byte[] buffer = new byte[bufferedSize];
                int current;
                int size = 0;
                boolean first = true;
                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                int read = dataInputStream.read(buffer, 0, 1024);
                JSONObject receivedJSON = new JSONObject(new String(buffer,0, 1024, StandardCharsets.UTF_8));
                System.out.println("Copy Received:" + receivedJSON);
                String path = new File("").getAbsolutePath() + "/SDFS_files/" + fileName + "_" + i;
                if (receivedJSON.has(FILE_TYPE) && receivedJSON.get(FILE_TYPE) == DICT) {
                    path += ".zip";
                }
                while((current = dis.read(buffer)) != -1){
                    if (first) {
                        localFileDos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path, false)));
                        first = false;
                    }
                    localFileDos.write(buffer, 0, current);
                    size += current;
                }
                if (localFileDos != null && size > 0) {
                    localFileDos.flush();
                    localFileDos.close();
                }
                if (receivedJSON.has(FILE_TYPE) && receivedJSON.get(FILE_TYPE) == DICT) {
                    String dest = new File("").getAbsolutePath() + "/SDFS_files";
                    tryUnZip(path, dest, fileName + "_" + i);
                }
            }
            storedFile.put(fileName, new SDFSFileInfo(new Timestamp(System.currentTimeMillis()), 1));
            return true;
        }
    }

    public class InferenceTask implements Callable<List<String>> {

        String inputFile;
        String jobID;
        String model;
        int batchNum;
        int batchSize;

        public InferenceTask(String inputFile, String jobID, String model, int batchNum, int batchSize) throws IOException {
            this.inputFile = inputFile;
            this.jobID = jobID;
            this.model = model;
            this.batchNum = batchNum;
            this.batchSize = batchSize;
        }

        @Override
        public List<String> call() {
            Process proc;
            try {
//                System.out.println("========Inference process starts=========");
                String execution = "";
                if (model.equals("number")) {
                    execution = "./predict.sh " + inputFile + " " + jobID + " " + model + " " + batchNum + " " + implicitBatchSizeNumber;
                } else {
                    execution = "./predict.sh " + inputFile + " " + jobID + " " + model + " " + batchNum + " " + implicitBatchSizeWeather;
                }
//                String execution = "./predict.sh " + inputFile + " " + jobID + " " + model + " " + batchNum + " " + batchSize;
                //System.out.println(execution);
                proc = Runtime.getRuntime().exec(execution);
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                String line = null;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                }
                in.close();
                proc.waitFor();
//                System.out.println("========Inference process finished=========");

                // Return inference results
                FileInputStream inputStream = new FileInputStream(new File("").getAbsolutePath() + "/prediction/" + jobID);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                List<String> res = new ArrayList<>();
                String str = null;
                while((str = bufferedReader.readLine()) != null)
                {
                    res.add(str);
                }
                inputStream.close();
                bufferedReader.close();
                return res;

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }



    public class Master implements Runnable {
        ServerSocket serverSocket;

        @Override
        public void run() {
            try {
                serverSocket = new ServerSocket(MASTERPORT);
                while (true) {
                    new Thread(new MasterHandler(serverSocket.accept())).start();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class MasterHandler implements Runnable{

        private Socket socket;

        public MasterHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            InputStreamReader isr;
            OutputStreamWriter osw;
            BufferedReader bufferedReader = null;
            BufferedWriter bufferedWriter = null;
            DataInputStream dis = null;

            try {
                isr = new InputStreamReader(socket.getInputStream());
                bufferedReader = new BufferedReader(isr);
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);
                dis = new DataInputStream(socket.getInputStream());
                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                byte[] buffer = new byte[4*1024];
                int read = dataInputStream.read(buffer, 0, 1024);
                //System.out.println(new String(buffer,0, 1024, StandardCharsets.UTF_8));
                if (new String(buffer,0, 1024, StandardCharsets.UTF_8).trim() == null || new String(buffer,0, 1024, StandardCharsets.UTF_8).trim().length() == 0) return;
                JSONObject receivedJSON = new JSONObject(new String(buffer,0, 1024, StandardCharsets.UTF_8).trim());

                if (receivedJSON.has(TYPE)) {
                    String type = receivedJSON.getString(TYPE);
                    switch (type) {
                        case GET: {
                            if (receivedJSON.has(IS_GETTING_VERSION)) {
                                if (receivedJSON.getBoolean(IS_GETTING_VERSION)) {
                                    new Thread(new LogRecorder(GET_VERSION, receivedJSON.getString(HOST), true, receivedJSON.getString(FILENAME))).start();
                                } else {
                                    new Thread(new LogRecorder(GET, receivedJSON.getString(HOST), true, receivedJSON.getString(FILENAME))).start();
                                }
                            }

                            System.out.println("======SDFS GET======");
                            JSONObject json = new JSONObject();
                            if (!fileMetaInfo.containsKey(receivedJSON.getString(FILENAME))) {
                                System.out.println("FILE NAME NOT EXIST!!!!!");
                                json.put(TYPE, GETREPLY);
                                json.put(GET_SUCCESS, false);
                                json.put(LATEST_VERSION, storedFile.get(receivedJSON.getString(FILENAME)));
                                bufferedWriter.write(json + "\n");
                                bufferedWriter.flush();
                                break;
                            }
                            List<String> fileHolders = fileMetaInfo.get(receivedJSON.getString(FILENAME));

                            JSONArray jsonArray = new JSONArray();
                            for (int i = 0, maxReturn = 2; i < fileHolders.size() && maxReturn > 0; ++i, --maxReturn) {
                                String holder = fileHolders.get(i);
                                if (membershipList.get(holder).getAlive()) {
                                    jsonArray.put(holder);
                                }
                            }
                            json.put(TYPE, GETREPLY);
                            json.put(GET_SUCCESS, true);
                            json.put(READCONTACTLIST, jsonArray);

                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            System.out.println("======SDFS GET COMPLETE======");
                            break;
                        }

                        case PUT: {
                            String sdfsFile = receivedJSON.getString(SDFSFILE);
                            new Thread(new LogRecorder(PUT, receivedJSON.getString(HOST), true, sdfsFile)).start();
                            System.out.println("======SDFS PUT======");
                            JSONArray fileHolders = new JSONArray();
                            int numverOfVersion = fileToLatestVersion.getOrDefault(sdfsFile, 0);
                            if (fileMetaInfo.containsKey(sdfsFile)) {
                                fileToLatestVersion.put(sdfsFile, numverOfVersion + 1);
                                List<String> temp = fileMetaInfo.get(sdfsFile);
                                for (String ss: temp) {
                                    fileHolders.put(ss);
                                }
                            } else {
                                List<String> newFileHolders = new ArrayList<>();
                                fileHolders = new JSONArray();
                                // Get current master server
                                FileReader file = new FileReader(new File("").getAbsolutePath() + "/utils/master.txt");
                                BufferedReader buf = new BufferedReader(file);
                                String master = buf.readLine();
                                System.out.println("========" + master + "=======");
                                int hash = Math.abs(sdfsFile.hashCode());
                                int start = hash % 10;
                                for (int i = 0; i < 10 && fileHolders.length() < 4; i++) {
                                    int index = (start + i) % 10;
                                    if (membershipList.get(GROUP[index]).getAlive()) {
                                        fileHolders.put(GROUP[index]);
                                        newFileHolders.add(GROUP[index]);
                                        List<String> fileList = serverStoreFiles.getOrDefault(GROUP[index], new ArrayList<String>());
                                        fileList.add(sdfsFile);
                                        serverStoreFiles.put(GROUP[index], fileList);
                                    }
                                }
                                fileMetaInfo.put(sdfsFile, newFileHolders);
                                fileToLatestVersion.put(sdfsFile, 1);
                            }
                            for (String backup : MASTER_BACKUP) {
                                if (backup.equals(HOSTNAME) || !membershipList.get(backup).getAlive()) {
                                    continue;
                                }
                                JSONObject json = new JSONObject();
                                json.put(TYPE, BACK_UP_PUT);
                                json.put(STOREHOSTS, fileHolders);
                                json.put(LATEST_VERSION, fileToLatestVersion.get(sdfsFile));
                                json.put(SDFSFILE, sdfsFile);
                                sender.sendRequest(json, backup, BACKUPPORT);
                            }
                            JSONObject json = new JSONObject();
                            json.put(TYPE, PUT_REPLY);
                            json.put(STOREHOSTS, fileHolders);
                            json.put(LATEST_VERSION, fileToLatestVersion.get(sdfsFile));
                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            System.out.println("======SDFS PUT COMPLETE======");
                            break;
                        }

                        case GET_NUM_VERSION: {
                            JSONObject json = new JSONObject();
                            json.put(TYPE, GET_NUM_VERSION_REPLY);
                            json.put(NUM_VERSION, fileToLatestVersion.get(receivedJSON.getString(FILENAME)));
                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case DELETE: {
                            String sdfsFile = receivedJSON.getString(SDFSFILE);
                            new Thread(new LogRecorder(DELETE, receivedJSON.getString(HOST), true, sdfsFile)).start();
                            System.out.println("======SDFS DELETE======");
                            if (!fileMetaInfo.containsKey(sdfsFile)) {
                                System.out.println("No such a file, can not delete");
                                JSONObject json = new JSONObject();
                                json.put(TYPE, DELETE_NO_FILE);
                                json.put(DELETE_RESULT, false);
                                bufferedWriter.write(json + "\n");
                                bufferedWriter.flush();
                                break;
                            } else {
                                boolean success = true;
                                List<String> fileHolders = fileMetaInfo.get(sdfsFile);
                                for (String fileHolder : fileHolders) {
                                    if (membershipList.get(fileHolder).getAlive()) {
                                        JSONObject deleteFile = new JSONObject();
                                        deleteFile.put(TYPE, DELETE);
                                        deleteFile.put(SDFSFILE, sdfsFile);
                                        JSONObject result = sender.sendRequest(deleteFile, fileHolder, RESPONDERRPORT);
                                        if (!result.getBoolean(DELETE_RESULT)) {
                                            JSONObject json = new JSONObject();
                                            json.put(TYPE, DELETE_FAIL);
                                            json.put(DELETE_RESULT, false);
                                            bufferedWriter.write(json + "\n");
                                            bufferedWriter.flush();
                                            success = false;
                                            break;
                                        }
                                    }
                                }
                                if (success) {
                                    List<String> servers = fileMetaInfo.get(sdfsFile);
                                    fileMetaInfo.remove(sdfsFile);
                                    fileToLatestVersion.remove(sdfsFile);
                                    // Master needs to update file info to alive back up machines
                                    JSONObject json = new JSONObject();
                                    json.put(TYPE, DELETE_RESULT);
                                    json.put(DELETE_RESULT, true);
                                    bufferedWriter.write(json + "\n");
                                    bufferedWriter.flush();
                                    for (String server : servers) {
                                        if (serverStoreFiles.containsKey(server)) {
                                            serverStoreFiles.get(server).remove(sdfsFile);
                                        }
                                    }
                                    for (String backup : MASTER_BACKUP) {
                                        if (backup.equals(HOSTNAME) || !membershipList.get(backup).getAlive()) {
                                            continue;
                                        }
                                        JSONObject deleteRequest = new JSONObject();
                                        deleteRequest.put(TYPE, BACK_UP_DELETE);
                                        deleteRequest.put(SDFSFILE, sdfsFile);
                                        sender.sendRequest(deleteRequest, backup, BACKUPPORT);
                                    }
                                }
                            }
                            System.out.println("======SDFS DELETE COMPLETE======");
                            break;
                        }

                        case LS: {
                            String sdfsFile = receivedJSON.getString(SDFSFILE);
                            new Thread(new LogRecorder(LS, receivedJSON.getString(HOST), true, sdfsFile)).start();
                            JSONObject result = new JSONObject();
                            if (!fileMetaInfo.containsKey(sdfsFile)) {
                                bufferedWriter.write(result + "\n");
                                bufferedWriter.flush();
                            } else {
                                List<String> list = fileMetaInfo.get(sdfsFile);
                                JSONArray arr = new JSONArray();
                                for (String str: list) {
                                    arr.put(str);
                                }
                                result.put(LSLIST, arr);
                                bufferedWriter.write(result + "\n");
                                bufferedWriter.flush();
                            }
                            break;
                        }

                        case INFERENCE_REQUEST: {
                            String inputFile = receivedJSON.getString(FILENAME);
                            String model = receivedJSON.getString(MODEL);
                            int batchSize = receivedJSON.getInt(BATCH_SIZE);
                            JSONObject json = new JSONObject();
                            json.put(TYPE, INFERENCE_REPLY);
                            String currentJobID = (model.equals("number"))? "1" : "2";
//                            ++globalJobID;
                            ExecutorService executorService = Executors.newFixedThreadPool(1);
                            List<Callable<Boolean>> callables = new ArrayList<>();
                            callables.add(new InferenceAllocation(inputFile, currentJobID, model, batchSize));
                            List<Future<Boolean>> futures = executorService.invokeAll(callables);
                            boolean isSuccess = futures.get(0).get();
                            if (isSuccess) {
                                json.put(INFERENCE_SUCCESS, true);
                                sdfs.put(currentJobID, "inference_result_" + currentJobID);
                                System.out.println("=======Finished sending inference result to SDFS========");
                                System.out.println("=======The inference result is stored as: " + "inference_result_" + currentJobID + "========");
                            } else {
                                json.put(INFERENCE_SUCCESS, false);
                            }

                            // Reply with inference result
                            bufferedWriter.write(json + "\n");
                            bufferedWriter.flush();
                            break;

                        }

                        case INFERENCE_RESULT: {
                            String jobID = receivedJSON.getString(JOB_ID);
                            String jobResponder = receivedJSON.getString(HOST);
                            String hostname = receivedJSON.getString(JOB_TAKER);
                            File deleteFolder = new File(new File("").getAbsolutePath() + "/temporary/");
                            File[] files = deleteFolder.listFiles();
                            if(files!=null) {
                                for(File f: files) {
                                    f.delete();
                                }
                            }
                            deleteFolder.delete();
                            Files.createDirectories(Paths.get(new File("").getAbsolutePath() + "/temporary/" + jobID + "/"));
                            FileOutputStream fileOutputStream = new FileOutputStream(new File("").getAbsolutePath() + "/temporary/" + jobID + "/" + jobID + jobResponder);
                            int bytes;
                            long size = receivedJSON.getLong(FILE_SIZE);
                            //System.out.println("file size:" + size);
                            while (size > 0 && (bytes = dis.read(buffer,0,(int)Math.min(size, 4096))) != -1) {
                                fileOutputStream.write(buffer,0,bytes);
                                size -= bytes;
                            }
                            fileOutputStream.close();
                            System.out.println("=========Received Inference Result==========");
                            break;
                        }

                        case C1_QUERY_RATE: {
                            boolean isBoth = receivedJSON.getBoolean(BOTH_HANDLED);
                            JSONObject jsonObject = new JSONObject();
                            if (!isBoth) {
                                String jobID = receivedJSON.getString(JOB_ID);
                                int totalQueriesProcess = jobMetaInfo.get(jobID).getProcessed().size();
                                double queryRate = 0.0;
                                if (jobMetaInfo.get(jobID).getModel().equals("number")) {
                                    if (numberQueryRate.size() >= 6) {
                                        while (numberQueryRate.size() > 6) {
                                            numberQueryRate.removeFirst();
                                        }
                                        queryRate = (double)(numberQueryRate.getLast() - numberQueryRate.getFirst()) / (AVG_TIME * NUM_BASE);
                                    } else {
                                        queryRate = (double)numberQueryRate.getLast() / (AVG_TIME * NUM_BASE);
                                    }
                                } else if (jobMetaInfo.get(jobID).getModel().equals("weather")) {
                                    if (weatherQueryRate.size() >= 6) {
                                        while (weatherQueryRate.size() > 6) {
                                            weatherQueryRate.removeFirst();
                                        }
                                        queryRate = (double)(weatherQueryRate.getLast() - weatherQueryRate.getFirst()) / (AVG_TIME * WEA_BASE);
                                    } else {
                                        queryRate = (double)weatherQueryRate.getLast() / (AVG_TIME * WEA_BASE);
                                    }
                                }
                                jsonObject.put(QUERY_RATE, queryRate);
                                jsonObject.put(TOTAL_QUERIES_PROCESSED, totalQueriesProcess);
                            } else {
                                int totalQueriesProcessJob1 = jobMetaInfo.get("1").getProcessed().size();
                                int totalQueriesProcessJob2 = jobMetaInfo.get("2").getProcessed().size();
                                double queryRateJob1 = 0.0, queryRateJob2 = 0.0;
//                                System.out.println("numberQueryRate Size:" + numberQueryRate.size());
                                if (numberQueryRate.size() >= 6) {
                                    while (numberQueryRate.size() > 6) {
                                        numberQueryRate.removeFirst();
                                    }
                                    queryRateJob1 = (double)(numberQueryRate.getLast() - numberQueryRate.getFirst()) / (AVG_TIME * NUM_BASE);
                                } else {
                                    queryRateJob1 = (double)numberQueryRate.getLast() / (AVG_TIME);
                                }
//                                System.out.println("weatherQueryRate Size:" + weatherQueryRate.size());
                                if (weatherQueryRate.size() >= 6) {
                                    while (weatherQueryRate.size() > 6) {
                                        weatherQueryRate.removeFirst();
                                    }
                                    queryRateJob2 = (double)(weatherQueryRate.getLast() - weatherQueryRate.getFirst()) / (AVG_TIME * WEA_BASE);
                                } else {
                                    queryRateJob2 = (double)weatherQueryRate.getLast() / (AVG_TIME * WEA_BASE);
                                }
                                jsonObject.put(QUERY_RATE_JOB1, queryRateJob1);
                                jsonObject.put(QUERY_RATE_JOB2, queryRateJob2);
                                jsonObject.put(TOTAL_QUERIES_PROCESSED_JOB1, totalQueriesProcessJob1);
                                jsonObject.put(TOTAL_QUERIES_PROCESSED_JOB2, totalQueriesProcessJob2);
                            }
                            bufferedWriter.write(jsonObject + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case C5_VM_ASSIGN: {
                            JSONObject jsonObject = new JSONObject();
                            int numJobs = jobMetaInfo.size();
                            jsonObject.put(NUM_JOBS, numJobs);
                            if (numJobs == 1) {
                                JSONArray jsonArray = new JSONArray();
                                Iterator<Map.Entry<String, MachineInferenceStatus>> iterator = machineToJob.entrySet().iterator();
                                Iterator<Map.Entry<String, JobStatus>> iterator2 = jobMetaInfo.entrySet().iterator();
                                String jobID = "1";
                                if (iterator2.hasNext()) {
                                    Map.Entry<String, JobStatus> entry2 = iterator2.next();
                                    jobID = entry2.getKey();
                                }
                                while(iterator.hasNext()){
                                    Map.Entry<String, MachineInferenceStatus> entry = iterator.next();
                                    if (membershipList.get(entry.getKey()).getAlive() && !entry.getValue().isAvailable()) {
                                        jsonArray.put(entry.getKey());
                                    }
                                }
                                jsonObject.put(VM_ASSIGNED, jsonArray);
                                jsonObject.put(JOB_ID, jobID);
                            } else if (numJobs == 2) {
                                JSONArray jsonArray1 = new JSONArray();
                                JSONArray jsonArray2 = new JSONArray();
                                Iterator<Map.Entry<String, MachineInferenceStatus>> iterator = machineToJob.entrySet().iterator();
                                while(iterator.hasNext()){
                                    Map.Entry<String, MachineInferenceStatus> entry = iterator.next();
                                    if (membershipList.get(entry.getKey()).getAlive() && !entry.getValue().isAvailable()) {
                                        if (entry.getValue().getJobID().equals("1")) {
                                            jsonArray1.put(entry.getKey());
                                        } else if (entry.getValue().getJobID().equals("2")) {
                                            jsonArray2.put(entry.getKey());
                                        }
                                    }
                                }
                                jsonObject.put(VM_ASSIGNED_JOB1, jsonArray1);
                                jsonObject.put(VM_ASSIGNED_JOB2, jsonArray2);
                            }
                            bufferedWriter.write(jsonObject + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case C4_QUERY_RESULT: {
                            String jobID = receivedJSON.getString(JOB_ID);
                            Path filePath = Paths.get(new File("").getAbsolutePath() + "/local_files/" + jobID);
                            String fileContent = "";
                            byte[] bytes = Files.readAllBytes(filePath);
                            fileContent = new String (bytes);
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(INFERENCE_CONTENTS, fileContent);
                            bufferedWriter.write(jsonObject + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        case C2_STATISTICS: {
                            String jobID = receivedJSON.getString(JOB_ID);
                            List<Long> processingTimes = new ArrayList<>();
                            for (Long l: jobMetaInfo.get(jobID).getQueryProcessingTimes()) {
                                processingTimes.add(l);
                            }

                            int size = processingTimes.size();

                            // Average
                            long sum = 0;
                            for (Long l: processingTimes) {
                                sum += l;
                            }
                            double average = ((double)sum / size);

                            // Standard deviation
                            double variance = 0;
                            for (long l: processingTimes) {
                                variance = variance + (Math.pow((l - average), 2));
                            }
                            variance = variance / size;
                            double std = Math.sqrt(variance);
                            Collections.sort(processingTimes);

                            // Median
                            double median = 0;
                            if(size % 2 != 1){
                                median = ((double)processingTimes.get(size/2-1) + processingTimes.get(size/2))/2;
                            } else {
                                median = processingTimes.get((size-1)/2);
                            }

                            // Percentiles
                            long ninetyPercentile = processingTimes.get((int)Math.ceil(90 / 100.0 * size) - 1);
                            long ninetyFivePercentile = processingTimes.get((int)Math.ceil(95 / 100.0 * size) - 1);
                            long ninetyNinePercentile = processingTimes.get((int)Math.ceil(99 / 100.0 * size) - 1);

                            // Reply
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(AVERAGE, average);
                            jsonObject.put(STD, std);
                            jsonObject.put(MEDIAN, median);
                            jsonObject.put(NINETY_PERCENTILE, ninetyPercentile);
                            jsonObject.put(NINETY_FIVE_PERCENTILE, ninetyFivePercentile);
                            jsonObject.put(NINETY_NINE_PERCENTILE, ninetyNinePercentile);
                            bufferedWriter.write(jsonObject + "\n");
                            bufferedWriter.flush();
                            break;
                        }

                        default: {
                            System.out.println("Undefined Json Type!!");
                        }
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
                //throw new RuntimeException(e);
            } finally {
                try {
                    if (bufferedWriter != null) bufferedWriter.close();
                    if (bufferedReader != null) bufferedReader.close();
                    socket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }
    }



    public class InferenceAllocation implements Callable<Boolean> {

        String inputFile;
        String jobID;
        String model;
        int batchSize;


        public InferenceAllocation(String inputFile, String jobID, String model, int batchSize) {
            this.inputFile = inputFile;
            this.jobID = jobID;
            this.model = model;
            this.batchSize = batchSize;
        }

        @Override
        public Boolean call() {

            try {
                if (firstInference) {
                    firstInference = false;
                    for (String host : GROUP) {
                        JSONObject stopPyRequest = new JSONObject();
                        stopPyRequest.put(TYPE, STOP_PY);
                        if (membershipList.get(host).getAlive()) {
                            sender.sendRequest(stopPyRequest, host, RESPONDERRPORT);
                        }
                    }
                }

                // Check existence of file for storing inference results
                File file = new File(new File("").getAbsolutePath() + "/local_files/" + jobID);
                if(!file.exists()){
                    file.createNewFile();
                } else {
                    PrintWriter writer = new PrintWriter(file);
                    writer.print("");
                    writer.close();
                }


                // Check number of files in directory
                File dir = new File(new File("").getAbsolutePath() + "/SDFS_files/" + inputFile);
                File list[] = dir.listFiles();
                int fileNumber = list.length;
                int numberOfBatch = 1;
                if (model.equals("number")) {
                    //System.out.println("=========TEST HERE======" + implicitBatchSizeNumber + "============");
                    numberOfBatch = (int)Math.ceil((double)fileNumber / implicitBatchSizeNumber);
                } else if (model.equals("weather")) {
                    numberOfBatch = (int)Math.ceil((double)fileNumber / implicitBatchSizeWeather);
                }




                // Update jobMetaInfo with new job
                if (!jobMetaInfo.containsKey(jobID)) {
                    jobMetaInfo.put(jobID, new JobStatus(inputFile, model, batchSize, numberOfBatch));
                }
                int curr = 0;

                System.out.println("======Number of files: " + fileNumber + "======");
                System.out.println("=====Number of batch: " + numberOfBatch + "=======");

                // Start calculating query rate
                new Thread(new QueryRateCompute(jobID)).start();

                // Process inference in batch
                while (curr < numberOfBatch) {
                    if (jobMetaInfo.get(jobID).getProcessed().contains(curr)) {
//                        System.out.println(jobID + "has already finished " + curr);
                        curr++;
                        continue;
                    }
                    // If there's only job currently running
                    if (jobMetaInfo.size() == 1) {
                        String machine = null;
                        Iterator<Map.Entry<String, MachineInferenceStatus>> it = machineToJob.entrySet().iterator();
                        while(it.hasNext()){
                            Map.Entry<String, MachineInferenceStatus> entry = it.next();
                            if (membershipList.get(entry.getKey()).getAlive()
                                    && entry.getValue().isAvailable()) {
                                machine = entry.getKey();
                                break;
                            }
                        }
                        if (machine != null) {
                            machineToJob.get(machine).setAvailable(false);
                            machineToJob.get(machine).setJobID(jobID);
                            machineToJob.get(machine).setIdx(curr);
                            machineToJob.get(machine).setModel(model);
                            machineToJob.get(machine).setInputFile(inputFile);
                            machineToJob.get(machine).setBatchSize(batchSize);
                            if (model.equals("number")) {
                                new Thread(new BatchInference(inputFile, jobID, model, implicitBatchSizeNumber, curr, machine, System.currentTimeMillis())).start();
                            } else if (model.equals("weather")) {
                                new Thread(new BatchInference(inputFile, jobID, model, implicitBatchSizeWeather, curr, machine, System.currentTimeMillis())).start();
                            }
                            curr++;
                        }
                    } else if (jobMetaInfo.size() == 2) { // If there are two jobs currently running

                        // Count number of machines working on each job
                        int numMachinesOnJob1 = 0, numMachineOnJob2 = 0;
                        Iterator<Map.Entry<String, MachineInferenceStatus>> iterator = machineToJob.entrySet().iterator();
                        while(iterator.hasNext()){
                            Map.Entry<String, MachineInferenceStatus> entry = iterator.next();
                            if (membershipList.get(entry.getKey()).getAlive() && !entry.getValue().isAvailable()) {
                                if (entry.getValue().getJobID().equals("1")) numMachinesOnJob1++;
                                else if (entry.getValue().getJobID().equals("2")) numMachineOnJob2++;
                            }
                        }

                        // Check the ratio
                        boolean isAssignedToJob1 = ((double)numMachinesOnJob1 / numMachineOnJob2) < ((double)batchSizeNumber / batchSizeWeather);
                        String machine = null;

                        // Choose the next alive machine
                        Iterator<Map.Entry<String, MachineInferenceStatus>> it = machineToJob.entrySet().iterator();
                        while(it.hasNext()) {
                            Map.Entry<String, MachineInferenceStatus> entry = it.next();
                            if (membershipList.get(entry.getKey()).getAlive() && entry.getValue().isAvailable()) {
                                machine = entry.getKey();
                                if ((jobID.equals("1") && isAssignedToJob1) || (jobID.equals("2") && !isAssignedToJob1)) {
                                    machineToJob.get(machine).setAvailable(false);
                                    break;
                                }
                            }
                        }
                        if (machine != null) {
                            if (jobID.equals("1") && isAssignedToJob1) {
                                machineToJob.get(machine).setJobID(jobID);
                                machineToJob.get(machine).setIdx(curr);
                                machineToJob.get(machine).setModel(model);
                                machineToJob.get(machine).setInputFile(inputFile);
                                machineToJob.get(machine).setBatchSize(batchSize);
                                new Thread(new BatchInference(inputFile, jobID, model, implicitBatchSizeNumber, curr, machine, System.currentTimeMillis())).start();
                                curr++;
                            } else if (jobID.equals("2") && !isAssignedToJob1) {
                                machineToJob.get(machine).setJobID(jobID);
                                machineToJob.get(machine).setIdx(curr);
                                machineToJob.get(machine).setModel(model);
                                machineToJob.get(machine).setInputFile(inputFile);
                                machineToJob.get(machine).setBatchSize(batchSize);
                                new Thread(new BatchInference(inputFile, jobID, model, implicitBatchSizeWeather, curr, machine, System.currentTimeMillis())).start();
                                curr++;
                            }
                        }
                    }
                }

                // Check completeness of inference job

                int currentImpBatchSize = 1;
                if (jobID.equals("1")) {
                    currentImpBatchSize = implicitBatchSizeNumber;
                } else if (jobID.equals("2")) {
                    currentImpBatchSize=  implicitBatchSizeWeather;
                }
                ExecutorService executorServiceCheck = Executors.newFixedThreadPool(1);
                List<Callable<Boolean>> callable = new ArrayList<>();
                callable.add(new CheckJob(jobID, currentImpBatchSize));

                //System.out.println("=======Gonna check=====");

                List<Future<Boolean>> result = executorServiceCheck.invokeAll(callable);
                if (result.get(0).get()) {

                    System.out.println("========Total completed batch: " + jobMetaInfo.get(jobID).processed.size() + "======");

                    jobMetaInfo.remove(jobID);
                    return true;
                }
                return true;
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class BatchInference implements Runnable {

        String inputFile;
        String jobID;
        String model;
        String hostname;
        int batchSize;
        int batchNum;
        long startTime;


        public BatchInference(String inputFile, String jobID, String model, int batchSize, int batchNum, String hostname, long startTime) {
            this.inputFile = inputFile;
            this.jobID = jobID;
            this.model = model;
            this.batchSize = batchSize;
            this.batchNum = batchNum;
            this.hostname = hostname;
            this.startTime = startTime;
        }

        @Override
        public void run() {
            Socket socket = null;
            InputStreamReader isr;
            OutputStreamWriter osw;
            BufferedReader bufferedReader = null;
            BufferedWriter bufferedWriter = null;
            try {
                socket = new Socket(hostname, RESPONDERRPORT);
                isr = new InputStreamReader(socket.getInputStream());
                bufferedReader = new BufferedReader(isr);
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);
                JSONObject inferenceRequest = new JSONObject();
                inferenceRequest.put(TYPE, INFERENCE);
                inferenceRequest.put(FILENAME, inputFile);
                inferenceRequest.put(JOB_ID, jobID);
                inferenceRequest.put(MODEL, model);
                inferenceRequest.put(BATCH_START_TIME, startTime);
                inferenceRequest.put(BATCH_SIZE, batchSize);
                inferenceRequest.put(BATCH_NUMBER, batchNum);
                bufferedWriter.write(inferenceRequest.toString() + "\n");
                bufferedWriter.flush();
                String s = bufferedReader.readLine();
                if (s == null) return;
                JSONObject receivedJSON = new JSONObject(s);
                if (receivedJSON.getBoolean(INFERENCE_SUCCESS)) {
                    JSONObject backupInference = new JSONObject();
                    long time = System.currentTimeMillis() - receivedJSON.getLong(BATCH_START_TIME);
                    if (jobMetaInfo.containsKey(jobID) && !jobMetaInfo.get(jobID).getProcessed().contains(batchNum)) {
                        // Write result to local file
                        File file = new File(new File("").getAbsolutePath() + "/local_files/" + jobID);
                        FileOutputStream fileOutputStream = new FileOutputStream(file,true);
                        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, "UTF-8");
                        JSONArray jsonArray = receivedJSON.getJSONArray(INFERENCE_ARRAY_REPLY);
                        for (int i = 0; i != jsonArray.length(); ++i) {
                            outputStreamWriter.write(jsonArray.getString(i) + "\n");
                        }
                        outputStreamWriter.close();
                        jobMetaInfo.get(jobID).getProcessed().add(batchNum);
                        jobMetaInfo.get(jobID).getQueryProcessingTimes().add(time);
                    }
                    machineToJob.get(hostname).setAvailable(true);
                    if (!HOSTNAME.equals(GROUP[1])) {
                        backupInference.put(TYPE, BACK_UP_INFERENCE);
                        backupInference.put(JOB_ID, jobID);
                        backupInference.put(BATCH_NUMBER, batchNum);
                        backupInference.put(FILENAME, inputFile);
                        if (jobID.equals("1")) {
                            backupInference.put(MODEL, "number");
                        } else {
                            backupInference.put(MODEL, "weather");
                        }
//                        backupInference.put(MODEL, model);
                        backupInference.put(BATCH_SIZE, batchSize);
                        backupInference.put(IMPLICIT_BATCH_SIZE_NUMBER, implicitBatchSizeNumber);
                        backupInference.put("batchWeather", batchSizeWeather);
                        backupInference.put("batchNum", batchSizeNumber);
                        backupInference.put(HOST, hostname);
                        backupInference.put(BATCH_TIME, time);
                        backupInference.put(INFERENCE_ARRAY_REPLY, receivedJSON.getJSONArray(INFERENCE_ARRAY_REPLY));
                        sender.sendRequest(backupInference, GROUP[1], BACKUPPORT);
                    }
                } else {
                    machineToJob.get(hostname).setAvailable(true);
                }
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public class CheckJob implements Callable<Boolean> {

        String currentJobID;
        int implicitBatchSize;

        public CheckJob(String currentJobID, int implicitBatchSize) {
            this.currentJobID = currentJobID;
            this.implicitBatchSize = implicitBatchSize;
        }

        @Override
        public Boolean call() throws Exception {



            boolean allFinished = false;
            int totalBatchNum = jobMetaInfo.get(currentJobID).getTotalBatchSize();



//            System.out.println("=======Start checking11111: " + totalBatchNum + "========");

            String inputFile = jobMetaInfo.get(currentJobID).getInputFile();
            String model = jobMetaInfo.get(currentJobID).getModel();
            int batchSize = jobMetaInfo.get(currentJobID).getBatchSize();

            while (!allFinished) {
                boolean currentAllFinished = true;
                Set<Integer> completed = jobMetaInfo.get(currentJobID).getProcessed();
                for (int i = 0; i < totalBatchNum; ++i) {
                    if (!completed.contains(i)) {
                        Iterator<Map.Entry<String, MachineInferenceStatus>> it = machineToJob.entrySet().iterator();
                        while(it.hasNext()){
                            Map.Entry<String, MachineInferenceStatus> entry = it.next();
                            if (membershipList.get(entry.getKey()).getAlive() && entry.getValue().isAvailable() && !entry.getKey().equals(MASTER)) {
                                String machine = entry.getKey();
                                machineToJob.get(machine).setAvailable(false);
                                machineToJob.get(machine).setJobID(currentJobID);
                                machineToJob.get(machine).setIdx(i);
                                machineToJob.get(machine).setModel(model);
                                machineToJob.get(machine).setInputFile(inputFile);
                                machineToJob.get(machine).setBatchSize(batchSize);
                                new Thread(new BatchInference(inputFile, currentJobID, model, implicitBatchSizeWeather, i, entry.getKey(), System.currentTimeMillis())).start();
                                break;
                            }
                        }
                        currentAllFinished = false;
                    }
                }
                allFinished = currentAllFinished;
                Thread.sleep(Check_Job_Timeout);
            }

            System.out.println("=====GET THROUGH CHECKING=====");

            return true;
        }
    }

    public class QueryRateCompute implements Runnable {

        String jobID;
        Deque<Integer> currentQueryRate;

        public QueryRateCompute(String jobID) {
            this.jobID = jobID;
            if (jobMetaInfo.get(jobID).getModel().equals("number")) {
                currentQueryRate = numberQueryRate;
            } else if (jobMetaInfo.get(jobID).getModel().equals("weather")) {
                currentQueryRate = weatherQueryRate;
            }
        }

        @Override
        public void run() {
            while (jobMetaInfo.containsKey(jobID)) {
                if (currentQueryRate.size() < 6) {
                    currentQueryRate.addLast(jobMetaInfo.get(jobID).getProcessed().size());
                } else {
                    while (currentQueryRate.size() > 6) {
                        currentQueryRate.removeFirst();
                    }
                    currentQueryRate.addLast(jobMetaInfo.get(jobID).getProcessed().size());
                }
                try {
                    Thread.sleep(QUERY_RATE_INTERVAL);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public class CopyFileBetweenMachine implements Runnable {

        String failedHostname;
        List<String> fileNames;
        List<String> fileContacts;
        List<Integer> fileVersions;
        Boolean success = false;

        public CopyFileBetweenMachine(String failedHostname, List<String> fileNames, List<String> fileContacts, List<Integer> fileVersions) {
            this.failedHostname = failedHostname;
            this.fileNames = fileNames;
            this.fileContacts = fileContacts;
            this.fileVersions = fileVersions;
        }

        @Override
        public void run() {

            Socket socket = null;
            InputStreamReader isr;
            OutputStreamWriter osw;
            BufferedReader bufferedReader = null;
            BufferedWriter bufferedWriter = null;

            // Find the index of the failed machine in GROUP list
            int curr;
            for (curr = 0; curr < GROUP.length; ++curr) {
                if (GROUP[curr].equals(failedHostname)) {
                    break;
                }
            }
            curr = (curr + 4) % GROUP.length;

            // Copy files originally stored in failed machine to another machine
            while (!success) {
                try {
                    long cur = System.currentTimeMillis();
//                    todo:|| GROUP[curr].equals(MASTER)
                    while (!membershipList.get(GROUP[curr]).getAlive()) {
                        curr = (curr + 1) % GROUP.length;
                    }
                    socket = new Socket(GROUP[curr], RESPONDERRPORT);
                    socket.setSoTimeout(COPY_FILE_TIMEOUT);
                    isr = new InputStreamReader(socket.getInputStream());
                    bufferedReader = new BufferedReader(isr);
                    osw = new OutputStreamWriter(socket.getOutputStream());
                    bufferedWriter = new BufferedWriter(osw);
                    JSONObject copyRequest = new JSONObject();
                    copyRequest.put(TYPE, COPY_FILES);
                    JSONArray fileNameArray = new JSONArray();
                    JSONArray fileContactsArray = new JSONArray();
                    JSONArray fileVersionsArray = new JSONArray();
                    for (int i = 0; i < fileNames.size(); ++i) {
                        fileNameArray.put(fileNames.get(i));
                        fileContactsArray.put(fileContacts.get(i));
                        fileVersionsArray.put(fileVersions.get(i));
                    }
                    copyRequest.put(FILES_TO_LATEST_VERSION, fileVersionsArray);
                    copyRequest.put(FILES_TO_BE_COPIED, fileNames);
                    copyRequest.put(FILE_CONTACT, fileContactsArray);
                    bufferedWriter.write(copyRequest.toString() + "\n");
                    bufferedWriter.flush();
                    String s = bufferedReader.readLine();
                    JSONObject receivedJSON = new JSONObject(s);

                    // If received ACK, update meta information
                    if (receivedJSON.has(COPY_FILE_SUCCESS) && receivedJSON.getBoolean(COPY_FILE_SUCCESS)) {
                        for (String file: fileNames) {
                            fileMetaInfo.get(file).add(GROUP[curr]);
                        }
                        List<String> storedFiles = serverStoreFiles.getOrDefault(GROUP[curr], new ArrayList<>());
                        for (String file: fileNames) {
                            storedFiles.add(file);
                        }
                        serverStoreFiles.put(GROUP[curr], storedFiles);
                        success = true;
                        System.out.println("=========File Copy Uses:" + (System.currentTimeMillis() - cur) + "ms=========");
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println("FILE COPY TIMEOUT, TRY ANOTHER MACHINE TO COPY FILE");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void start() throws IOException {
        Sender sender = new Sender();
        Receiver receiver = new Receiver();
        TimeoutChecker timeoutChecker = new TimeoutChecker();
        new Thread(receiver).start();
        new Thread(sender).start();
        new Thread(timeoutChecker).start();
        Responder responder = new Responder();
        if (MASTER_BACKUP.contains(HOSTNAME)) {
            Master master = new Master();
            new Thread(master).start();
        }
        BackUpHandler backUpHandler = new BackUpHandler();
        new Thread(backUpHandler).start();
        new Thread(responder).start();
    }

    public static void main(String[] args) throws IOException {
        DeleteFilesInDir deleteFilesInDir = new DeleteFilesInDir();
        deleteFilesInDir.deleteFiles(new File("").getAbsolutePath() + "/prediction");
        deleteFilesInDir.deleteFiles(new File("").getAbsolutePath() + "/temporary");
        Server server = new Server();
        server.start();
        server.run();
    }
    private void run() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please input your command: ");
        // receive the input command from user
        while (scanner.hasNext()) {
            String command = scanner.nextLine();
            command = command.trim();
            if (command.equalsIgnoreCase("JOIN")) {
                id = System.currentTimeMillis() + HOSTNAME.substring(13, 15);

                // Delete all files before joining into the system
                File dir = new File(new File("").getAbsolutePath() + "/SDFS_files");
                if (dir.exists() && dir != null) {
                    File[] files = dir.listFiles();
                    for (File file: files)
                        file.delete();
                }

                // Join into the system
                MemberJoin memberJoin = new MemberJoin();
                new Thread(memberJoin).start();
                System.out.println("Join.");
            } else if (command.equalsIgnoreCase("LEAVE")) {
                MemberLeave memberLeave = new MemberLeave();
                new Thread(memberLeave).start();
                System.out.println("Leave.");
            } else if (command.equalsIgnoreCase("SELFID")) {
                System.out.println("Self's id: " + id);
            } else if (command.equalsIgnoreCase("PRINT")) {
                for (String key : membershipList.keySet()) {
                    String member = "";
                    if (membershipList.get(key).getAlive()) member = key + " (" + membershipList.get(key).getId() + "): ";
                    else member = key + ": ";
                    System.out.println(member + (membershipList.get(key).getAlive()? "Alive": "Leave/Fail"));
                }
            } else if(command.equalsIgnoreCase("STORE")) {
                new Thread(new LogRecorder(STORE, HOSTNAME, true, "")).start();
                Set<String> stores = storedFile.keySet();
                System.out.println("Files currently being stored:");
                for (String file: stores) {
                    System.out.println(file);
                }
            } else {
                String[] argument = command.split(" ");
                if (argument[0].equalsIgnoreCase("PUT")) {
                    if (argument.length != 3) {
                        System.out.println("invalid input");
                    } else {
                        String localFile = argument[1];
                        String sdfsFile = argument[2];
                        new Thread(new LogRecorder(PUT, HOSTNAME, true, sdfsFile)).start();
                        //putLocalFile(localFile,sdfsFile);
                        File file = new File(new File("").getAbsolutePath() + "/local_files/" + localFile);
                        if (!file.exists()) {
                            System.out.println("Can not find this file!");
                        }
                        sdfs.put(localFile, sdfsFile);
                    }
                } else if (argument[0].equalsIgnoreCase("GET")) {
                    if (argument.length != 3) {
                        System.out.println("invalid input");
                    } else {
                        String sdfsFile = argument[1];
                        String localFile = argument[2];
                        new Thread(new LogRecorder(GET, HOSTNAME, true, sdfsFile)).start();
                        sdfs.get(localFile, sdfsFile, false, 0);
                    }

                } else if (argument[0].equalsIgnoreCase("DELETE")) {
                    if (argument.length != 2) {
                        System.out.println("invalid input");
                    } else {
                        String sdfsFile = argument[1];
                        new Thread(new LogRecorder(DELETE, HOSTNAME, true, sdfsFile)).start();
                        sdfs.delete(sdfsFile);
                    }
                } else if (argument[0].equalsIgnoreCase("LS")) {
                    if (argument.length != 2) {
                        System.out.println("invalid input");
                    } else {
                        String sdfsFile = argument[1];
                        new Thread(new LogRecorder(LS, HOSTNAME, true, sdfsFile)).start();
                        sdfs.Ls(sdfsFile);
                    }
                } else if (argument[0].equalsIgnoreCase("GET-VERSIONS")) {
                    if (argument.length != 4) {
                        System.out.println("invalid input");
                    } else {
                        String sdfsFile = argument[1];
                        String numVersion = argument[2];
                        try {
                            Integer.parseInt(numVersion);
                        } catch (Exception e) {
                            System.out.println("Oops, the number of version should be an INTEGER");
                        }
                        String localFile = argument[3];
                        new Thread(new LogRecorder(GET_VERSION, HOSTNAME, true, sdfsFile)).start();
                        sdfs.get(localFile, sdfsFile, true, Integer.parseInt(numVersion));
                    }
                } else if (argument[0].equalsIgnoreCase("INFERENCE")) {
                    if (argument.length != 3) {
                        System.out.println("invalid input");
                    } else {
                        if (!argument[2].equals("weather") && !argument[2].equals("number")) {
                            System.out.println("invalid model");
                        } else {
                            String inputFile = argument[1];
                            String model = argument[2];
                            new Thread(new InferenceHandler(inputFile, model, 0)).start();
                        }
                    }
                } else if (argument[0].equalsIgnoreCase("C1")) {
                    if (argument.length == 2) {
                        String jobID = argument[1];
                        commandHandler.C1(jobID, false);
                    } else if (argument.length == 1) {
                        commandHandler.C1("", true);
                    } else {
                        System.out.println("invalid input");
                    }
                } else if (argument[0].equalsIgnoreCase("C5")) {
                    if (argument.length == 1) {
                        commandHandler.C5();
                    } else {
                        System.out.println("invalid input");
                    }
                } else if (argument[0].equalsIgnoreCase("C4")) {
                    if (argument.length != 2) {
                        System.out.println("invalid input");
                    } else {
                        String jobID = argument[1];
                        commandHandler.C4(jobID);
                    }
                } else if (argument[0].equalsIgnoreCase("C2")) {
                    if (argument.length != 2) {
                        System.out.println("invalid input");
                    } else {
                        String jobID = argument[1];
                        commandHandler.C2(jobID);
                    }
                } else if (argument[0].equalsIgnoreCase("C3")) {
                    if (argument.length != 3) {
                        System.out.println("invalid input");
                    } else {
                        String model = argument[1];
                        int batchSize = Integer.parseInt(argument[2]);
                        if (model.equals("number")) {
                            batchSizeNumber = batchSize;
                            commandHandler.C3("number", batchSize, TRICK);
                        } else if (model.equals("weather")) {
                            batchSizeWeather = batchSize;
                            implicitBatchSizeNumber = TRICK * batchSizeNumber / batchSizeWeather;
                            commandHandler.C3("weather", batchSize, implicitBatchSizeNumber);
                            //System.out.println("========" + implicitBatchSizeNumber + "========");
                        } else {
                            System.out.println("invalid input");
                        }
                    }
                } else {
                    System.out.println("Undefined Command.");
                }
            }
        }
    }
}
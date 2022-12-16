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
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

    public Server() throws IOException {
        File file = new File(new File("").getAbsolutePath() + "/master.txt");
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
            List<String> filesInFailedMachine = null;
            if (serverStoreFiles.containsKey(leaveHostname))
                filesInFailedMachine = serverStoreFiles.get(leaveHostname);

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
                                    File file = new File(new File("").getAbsolutePath() + "/master.txt");
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
                if ( filesInFailedMachine != null && filesInFailedMachine.size() > 0) {
                    List<String> fileNames = new ArrayList<>();
                    List<String> fileContacts = new ArrayList<>();
                    List<Integer> fileVersions = new ArrayList<>();
                    for (String file: filesInFailedMachine) {
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
                    Thread.sleep(TIMEOUT);
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
                System.out.println("read:" + read);
                JSONObject receivedJSON = new JSONObject(new String(buffer,0, 1024, StandardCharsets.UTF_8));
                System.out.println("responder handler:\t" + receivedJSON);
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
                            // Transfer file with specified version to requester
                            dis = new DataInputStream(new BufferedInputStream(new FileInputStream(versionedFileName)));
                            out = new DataOutputStream(socket.getOutputStream());
                            int current =0;
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

                            // Take action according to whether the file has already existed
                            if (!storedFile.containsKey(sdfsFile)) {
                                fileName = sdfsFile + "_" + (latestVersion);
                                storedFile.put(sdfsFile, new SDFSFileInfo(new Timestamp(System.currentTimeMillis()), latestVersion));
                            } else {
                                fileName = sdfsFile + "_" + latestVersion;
                                storedFile.get(sdfsFile).setLastUpdatedTime(new Timestamp(System.currentTimeMillis()));
                                storedFile.get(sdfsFile).setLatestVersion(latestVersion);
                            }
                            FileOutputStream fileOutputStream = new FileOutputStream(new File("").getAbsolutePath() + "/SDFS_files/" + fileName);
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
                            // Launch one thread for each file
                            ExecutorService executorService = Executors.newFixedThreadPool(nonExist.size());
                            List<Callable<Boolean>> callables = new ArrayList<>();
                            for (int i: nonExist) {
                                callables.add(new CopyFile(fileNames.getString(i), fileContacts.getString(i), fileVersions.getInt(i)));
                            }
                            List<Future<Boolean>> futures = executorService.invokeAll(callables);
                            boolean isSuccess = true;
                            for(int i = 0; i < futures.size(); ++i){
                                if (!futures.get(i).get()) {
                                    isSuccess = false;
                                    break;
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
                                    if (!fileHolders.contains((String) hosts.get(i))) {
                                        fileHolders.add((String) hosts.get(i));
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
            DataOutputStream localFileDos;
            OutputStreamWriter osw;
            BufferedWriter bufferedWriter;

            for (int i = 1; i <= latestVersion; ++i) {
                socket = new Socket(destination, RESPONDERRPORT);
                localFileDos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File("").getAbsolutePath() + "/SDFS_files/" + fileName + "_" + i, false)));
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);

                // Send the requested file name and version number to the file holder
                JSONObject getVersionsRequest = new JSONObject();
                getVersionsRequest.put(TYPE, GET_VERSION);
                getVersionsRequest.put(FILENAME, fileName);
                getVersionsRequest.put(VERSION_NUM, i);
                bufferedWriter.write(getVersionsRequest.toString() + "\n");
                bufferedWriter.flush();

                // Receive the file and store the file with given version to designated directory
                File file = new File(new File("").getAbsolutePath() + "/SDFS_files/" + fileName + "_" + i);
                // Create file if not exist
                if (!file.exists()) {
                    file.createNewFile();
                }
                dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                int bufferedSize = 1024 * 10;
                byte[] buffer = new byte[bufferedSize];
                int current;
                while((current = dis.read(buffer)) != -1){
                    localFileDos.write(buffer, 0, current);
                }
                localFileDos.flush();
            }
            storedFile.put(fileName, new SDFSFileInfo(new Timestamp(System.currentTimeMillis()), 1));
            return true;
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

            try {
                isr = new InputStreamReader(socket.getInputStream());
                bufferedReader = new BufferedReader(isr);
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);
                String s = bufferedReader.readLine();
                JSONObject receivedJSON = new JSONObject(s);

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
                        default: {
                            System.out.println("Undefined Json Type!!");
                        }
                    }
                }
            } catch (IOException e) {
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

            // Copy files originally stored in failed machine to another machine
            while (!success) {
                try {
                    long cur = System.currentTimeMillis();
                    curr = (curr + 4) % GROUP.length;
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
                } else {
                    System.out.println("Undefined Command.");
                }
            }
        }
    }
}
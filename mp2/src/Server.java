import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.*;
import static utils.Constant.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Server {
    private final Map<String, MemberStatus> membershipList = new ConcurrentHashMap<>();

    // store sockets
    private final Map<String, DatagramSocket> neighborSockets = new ConcurrentHashMap<>();
    //store host's name
    private final Map<String, InetAddress> neighborInetAddress = new ConcurrentHashMap<>();

    // Record ping time
    private final Map<String, Long> timeSet = new ConcurrentHashMap<>();
    private final String HOSTNAME;
    private String id;
    private int count = 0;

    CopyOnWriteArrayList<Integer> bandwidth;

//    private final String path = new File(new File("").getAbsolutePath()) + "/log";

//    private BufferedWriter br = new BufferedWriter(new FileWriter(new File(path + "/machine.txt")));

    public Server() throws IOException {
        HOSTNAME = InetAddress.getLocalHost().getHostName();
        id = String.valueOf((HOSTNAME + new Timestamp(System.currentTimeMillis()).toString()).hashCode());
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
//        Path dict = Paths.get(path);
//        if (!Files.exists(dict)) {
//            Files.createDirectory(dict);
//        }
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
                                count++;
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
                                new Thread(new LogRecorder((String)msg.get(TYPE), (String)msg.get(HOST))).start();
                                // Introducer needs to carry out more actions when receiving JOIN message
                                if (HOSTNAME.equals(INTRODUCER)) {
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
                                    // Multicast JOIN message to all members in the group
                                    for (String member : GROUP) {
                                        if (!member.equals(INTRODUCER)) {
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
            // Leave / fail, the hostname in msg is the leave/fail process's
            String leaveHostname = (String) msg.get(HOST);
            // Only disseminate the message if leave/fail process is alive in current membership list to avoid redundant message
            if (membershipList.get(leaveHostname).getAlive()) {
                // Record machine LEAVE/FAIL events
                new Thread(new LogRecorder((String)msg.get(TYPE), (String)msg.get(HOST))).start();
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
     * LogRecorder class is used to log machine JOIN/LEAVE/FAIL events
     */
    private class LogRecorder implements Runnable {
        private String event;
        private String hostname;

        public LogRecorder(String event, String hostname) {
            this.hostname = hostname;
            this.event = event;
        }

        @Override
        public void run() {
            try {
                String fileName = "machine.";
                if (HOSTNAME.charAt(13) == '0') fileName += HOSTNAME.charAt(14);
                else fileName += HOSTNAME.substring(13, 15);
                fileName += ".log";
                FileHandler fileHandler = new FileHandler(fileName, true);
                Logger logger = Logger.getLogger(LogRecorder.class.getName());
                SimpleFormatter formatter = new SimpleFormatter();
                fileHandler.setFormatter(formatter);
                logger.addHandler(fileHandler);
                logger.info(hostname + " " + event + "\n");
                fileHandler.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

//    private class BandwidthCalculator implements Runnable {
//        @Override
//        public void run() {
//            try {
//                Thread.sleep(60 * 1000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//            bandwidth = new CopyOnWriteArrayList<>();
//
//            try {
//                Thread.sleep(TIMEOUT);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    private void start() throws SocketException {
        Sender sender = new Sender();
        Receiver receiver = new Receiver();
        TimeoutChecker timeoutChecker = new TimeoutChecker();
        new Thread(receiver).start();
        new Thread(sender).start();
        new Thread(timeoutChecker).start();
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
            if (command.equalsIgnoreCase("JOIN")) {
                id = String.valueOf((HOSTNAME + new Timestamp(System.currentTimeMillis()).toString()).hashCode());
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
            }
            else {
                System.out.println("Undefined Command.");
            }
        }
    }
}
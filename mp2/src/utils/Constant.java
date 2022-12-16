package utils;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public final class Constant {
    public final static int PORT = 10000;
    // introducer
    public final static String INTRODUCER = "INTRODUCER_HOSTNAME";

    public final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // message type
    public final static String PING = "PING";
    public final static String JOIN = "JOIN";
    public final static String LEAVE = "LEAVE";
    public final static String FAIL = "FAIL";
    public final static String MEMBERSHIPLIST = "MEMBERSHIPLIST";

    public final static String ACK = "ACK";

    public final static long TIMEOUT = 1 * 1000;
    public final static String HOST = "hostname";
    public final static String TYPE = "type";
    public final static String ID = "id";
    public final static String IDLIST = "idList";
    public final static String SENDTime = "sendTime";
    public final static String[] GROUP = new String[] {
            "vmHostname..."
    };
    public final static Map<String, String[]> Neighbors = new HashMap<>();
    static {
//        initial ring-style membership list
    }
}

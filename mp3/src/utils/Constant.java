package utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Constant {
    public final static int PORT = 10000;
    public final static int INDICATOR = 18999;
    // introducer
    public static String INTRODUCER = "vmHostnames...";

    public final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // message type
    public final static String PING = "PING";
    public final static String JOIN = "JOIN";
    public final static String LEAVE = "LEAVE";
    public final static String FAIL = "FAIL";
    public final static String MEMBERSHIPLIST = "MEMBERSHIPLIST";
    public final static String ACK = "ACK";
    public final static long TIMEOUT = 2 * 1000;
    public final static String HOST = "hostname";
    public final static String TYPE = "type";
    public final static String ID = "id";
    public final static String IDLIST = "idList";
    public final static String SENDTime = "sendTime";
    public final static String NEW_MASTER = "NEW_MASTER";
    public final static String END = "END";
    public final static String TIME = "TIME";
    public final static String REQ_TIME = "REQ_TIME";






    public static String MASTER = "vmHostnames...";
    public final static List<String> MASTER_BACKUP = new ArrayList<>();
    public final static String GET = "GET";
    public final static String PUT = "PUT";
    public final static String PUTFILE = "PUTFILE";
    public final static String DELETE = "DELETE";
    public final static String BACK_UP_DELETE = "BACK_UP_DELETE";
    public final static String LS = "LS";
    public final static int MASTERPORT = 15000;
    public final static int RESPONDERRPORT = 15001;
    public final static int RECEIVE_FILE_PORT = 15002;
    public final static int BACKUPPORT = 15003;
    public final static String BACK_UP_PUT = "BACK_UP_PUT";
    public final static String FILENAME = "FILENAME";
    public final static String SDFSFILE = "SDFSFILE";
    public final static String READCONTACTLIST = "READCONTACTLIST";
    public final static String STOREHOSTS = "STOREHOSTS";
    public final static String FILEUPDATETIME = "FILEUPDATETIME";
    public final static String GETREPLY = "GETREPLY";
    public final static String PUT_REPLY = "PUT_REPLY";
    public final static String DELETE_NO_FILE = "DELETE_NO_FILE";
    public final static String DELETE_RESULT = "DELETE_RESULT";
    public final static String DELETE_FAIL = "DELETE_FAIL";
    public final static String FILEUPDATE = "FILEUPDATE";
    public final static String GETFILE = "GETFILE";

    public final static String LSLIST = "LSLIST";
    public final static String GET_SUCCESS = "GET_SUCCESS";
    public final static String GET_FILE_UPDATE_SUCCESS = "GET_FILE_UPDATE_SUCCESS";
    public final static String GET_VERSION = "GET_VERSION";
    public final static String VERSION_NUM = "VERSION_NUM";
    public final static String GET_NUM_VERSION = "GET_NUM_VERSION";
    public final static String NUM_VERSION = "NUM_VERSION";
    public final static String FILE_SIZE = "FILE_SIZE";
    public final static String LATEST_VERSION = "LATEST_VERSION";
    public final static String COPY_FILES = "COPY_FILES";
    public final static String FILES_TO_BE_COPIED = "FILES_TO_BE_COPIED";
    public final static String COPY_FILE_SUCCESS = "COPY_FILE_SUCCESS";
    public final static String GET_NUM_VERSION_REPLY = "GET_NUM_VERSION_REPLY";

    public final static String BACK_UP_JOIN = "BACK_UP_JOIN";
    public final static String FILE_META_INFO_KEY = "FILE_META_INFO_KEY";
    public final static String FILE_META_INFO_VALUE = "FILE_META_INFO_VALUE";

    public final static String FILE_VERSIONS_KEY = "FILE_VERSIONS_KEY";
    public final static String FILE_VERSIONS_VALUE = "FILE_VERSIONS_VALUE";
    public final static String COPY_FILE_REPLY = "COPY_FILE_REPLY";
    public final static String FILES_TO_LATEST_VERSION = "FILES_TO_LATEST_VERSION";
    public final static String FILE_CONTACT = "FILE_CONTACT";
    public final static String STORE = "STORE";
    public final static String IS_GETTING_VERSION  = "IS_GETTING_VERSION";
    public final static int COPY_FILE_TIMEOUT = 40 * 1000;


    public final static String[] GROUP = new String[] {
            "vmHostnames..."
    };
    public final static Map<String, String[]> Neighbors = new HashMap<>();
    static {
        // initial backup and neighbors
    }
}

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
    public final static String BACK_UP_INFERENCE = "BACK_UP_INFERENCE";
    public final static String LS = "LS";
    public final static int MASTERPORT = 15000;
    public final static int RESPONDERRPORT = 15001;
    public final static int RECEIVE_FILE_PORT = 15002;
    public final static int BACKUPPORT = 15003;
    public final static String BACK_UP_PUT = "BACK_UP_PUT";
    public final static String FILENAME = "FILENAME";
    public final static String CLIENT = "CLIENT";
    public final static String SDFSFILE = "SDFSFILE";
    public final static String FILE_TYPE = "FILE_TYPE";
    public final static String FILE = "FILE";
    public final static String DICT = "DICT";
    public final static String READCONTACTLIST = "READCONTACTLIST";
    public final static String STOREHOSTS = "STOREHOSTS";
    public final static String STOP_PY = "STOP_PY";
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
    public final static String ACTION = "ACTION";
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
    public final static String INFERENCE = "INFERENCE";
    public final static String INFERENCE_REQUEST = "INFERENCE_REQUEST";
    public final static String INFERENCE_REPLY = "INFERENCE_REPLY";
    public final static String INFERENCE_SUCCESS = "INFERENCE_SUCCESS";
    public final static String JOB_SUBMITTED = "JOB_SUBMITTED";
    public final static String INFERENCE_RESULT = "INFERENCE_RESULT";
    public final static String JOB_ID = "JOB_ID";
    public final static String MODEL = "MODEL";
    public final static String TOTAL_MACHINE = "TOTAL_MACHINE";
    public final static String MACHINE_NUMBER = "MACHINE_NUMBER";
    public final static String DIR_EXIST_REQUEST = "DIR_EXIST_REQUEST";
    public final static String IS_DIR_EXIST = "IS_DIR_EXIST";
    public final static String JOB_TAKER = "JOB_TAKER";
    public final static int Check_Job_Timeout = 1 * 1000;
    public final static String BATCH_SIZE = "BATCH_SIZE";
    public final static int TRICK = 35;
    public final static String BATCH_NUMBER = "BATCH_NUMBER";
    public final static String INFERENCE_ARRAY_REPLY = "INFERENCE_ARRAY_REPLY";
    public final static int QUERY_RATE_INTERVAL = 2 * 1000;

    public final static String C1_QUERY_RATE = "C1_QUERY_RATE";
    public final static String QUERY_RATE  = "QUERY_RATE";
    public final static String TOTAL_QUERIES_PROCESSED = "TOTAL_QUERIES_PROCESSED";
    public final static String BOTH_HANDLED = "BOTH_HANDLED";
    public final static String QUERY_RATE_JOB1  = "QUERY_RATE_JOB1";
    public final static String QUERY_RATE_JOB2  = "QUERY_RATE_JOB2";
    public final static String TOTAL_QUERIES_PROCESSED_JOB1 = "TOTAL_QUERIES_PROCESSED_JOB1";
    public final static String TOTAL_QUERIES_PROCESSED_JOB2 = "TOTAL_QUERIES_PROCESSED_JOB2";
    public final static String C5_VM_ASSIGN = "C5_VM_ASSIGN";
    public final static String NUM_JOBS = "NUM_JOBS";
    public final static String VM_ASSIGNED = "VM_ASSIGNED";
    public final static String VM_ASSIGNED_JOB1 = "VM_ASSIGNED_JOB1";
    public final static String VM_ASSIGNED_JOB2 = "VM_ASSIGNED_JOB2";
    public final static String C4_QUERY_RESULT = "C4_QUERY_RESULT";
    public final static String INFERENCE_CONTENTS = "INFERENCE_CONTENTS";
    public final static String BATCH_START_TIME = "BATCH_START_TIME";
    public final static int AVG_TIME = 10;
    public final static double NUM_BASE = 1;
    public final static double WEA_BASE = 0.75;
    public final static String BATCH_TIME = "BATCH_TIME";
    public final static String C2_STATISTICS = "C2_STATISTICS";
    public final static String AVERAGE = "AVERAGE";
    public final static String STD =  "STD";
    public final static String MEDIAN = "MEDIAN";
    public final static String NINETY_PERCENTILE = "NINETY_PERCENTILE";
    public final static String NINETY_FIVE_PERCENTILE  = "NINETY_FIVE_PERCENTILE";
    public final static String NINETY_NINE_PERCENTILE  = "NINETY_NINE_PERCENTILE";
    public final static String IMPLICIT_BATCH_SIZE_NUMBER = "IMPLICIT_BATCH_SIZE_NUMBER";
    public final static String C3_SETUP = "C3_SETUP";
    public final static String C3_REPLY = "C3_REPLY";





    public final static String[] GROUP = new String[] {
            "vmHostnames..."
    };
    public final static Map<String, String[]> Neighbors = new HashMap<>();
    static {
        // initial backup and neighbors
    }
}

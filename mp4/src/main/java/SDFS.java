import org.json.JSONArray;
import org.json.JSONObject;
import utils.Constant;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static utils.Constant.*;

public class SDFS {

    private SDFSSender sender = new SDFSSender();

    void get(String localFileName, String SDFSFileName, boolean version, int numVersions) {
        try {
            // Send GET request to master server to obtain the holder of the file
            JSONObject getRequest = new JSONObject();
            getRequest.put(TYPE, Constant.GET);
            getRequest.put(FILENAME, SDFSFileName);
            getRequest.put(HOST, InetAddress.getLocalHost().getHostName());
            getRequest.put(IS_GETTING_VERSION, version);
            long cur = System.currentTimeMillis();
            JSONObject receivedJSON = sender.sendRequest(getRequest, MASTER, MASTERPORT);

            // If the file does not exist in SDFS, return right away
            if (!receivedJSON.getBoolean(GET_SUCCESS)) {
                System.out.println("File does not exist in SDFS");
                return;
            }

            // Obtain the machine lists containing the holders the requested file
            JSONArray contactList = (JSONArray) receivedJSON.get(READCONTACTLIST);
            List<String> contacts = new ArrayList<>();
            for (int i = 0; i < contactList.length(); ++i) {
                contacts.add(contactList.getString(i));
            }

            // Send request to the holder of the file to obtain the last updated time
            Map<String, String> updatedTime = new HashMap<>();
            for (String contact: contacts) {
                JSONObject json = new JSONObject();
                json.put(TYPE, FILEUPDATETIME);
                json.put(FILENAME, SDFSFileName);
                JSONObject lastUpdate = sender.sendRequest(json, contact, RESPONDERRPORT);
                if (lastUpdate.getBoolean(GET_FILE_UPDATE_SUCCESS))
                    updatedTime.put(lastUpdate.getString(HOST), lastUpdate.getString(FILEUPDATE));
            }

            // If only one file holder is received, get file from this holder,
            // while if there are two file holders received, get file from the holder that holds the latest file
            String latestHolder = null;
            if (updatedTime.size() == 1) {
                latestHolder = updatedTime.entrySet().iterator().next().getKey();
            } else if (updatedTime.size() == 2) {
                latestHolder = Collections.max(updatedTime.entrySet(), Map.Entry.comparingByValue()).getKey();
            }

            // Get actual number of versions of the requested file
            JSONObject getNumVersionRequest = new JSONObject();
            getNumVersionRequest.put(TYPE, GET_NUM_VERSION);
            getNumVersionRequest.put(FILENAME, SDFSFileName);
            JSONObject getVersionNumReply = sender.sendRequest(getNumVersionRequest, MASTER, MASTERPORT);

            // If the actually number of versions if smaller than requested
            int actuallyNumVersion = getVersionNumReply.getInt(NUM_VERSION);
            if (actuallyNumVersion < numVersions) {
                System.out.println("Oops, there are only " + actuallyNumVersion + " versions of the file");
            }
            sender.getFile(localFileName, SDFSFileName, latestHolder, RESPONDERRPORT,
                    version, numVersions, actuallyNumVersion, cur);
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException(e);
        }
    }



    public void put(String localFile, String sdfsFile) {
        try {
            JSONObject putRequest = new JSONObject();
            putRequest.put(TYPE, PUT);
            putRequest.put(SDFSFILE, sdfsFile);
            putRequest.put(HOST, InetAddress.getLocalHost().getHostName());
            long cur = System.currentTimeMillis();
            JSONObject result = sender.sendRequest(putRequest, MASTER, MASTERPORT);
            JSONArray hosts = (JSONArray) result.get(STOREHOSTS);
            int latestVersion = result.getInt(LATEST_VERSION);
            System.out.println("=====PUT====" + latestVersion);
            sender.sendFile(hosts, localFile, sdfsFile, latestVersion, cur);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String sdfsFile) {
        try {
            JSONObject putRequest = new JSONObject();
            putRequest.put(TYPE, DELETE);
            putRequest.put(SDFSFILE, sdfsFile);
            putRequest.put(HOST, InetAddress.getLocalHost().getHostName());
            JSONObject result = sender.sendRequest(putRequest, MASTER, MASTERPORT);
            if (result.getBoolean(DELETE_RESULT)) {
                System.out.println("Delete file successfully");
            } else {
                System.out.println("Delete fails");
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void Ls(String sdfsFile) {
        try {
            JSONObject LsRequest = new JSONObject();
            LsRequest.put(TYPE, LS);
            LsRequest.put(SDFSFILE, sdfsFile);
            LsRequest.put(HOST, InetAddress.getLocalHost().getHostName());
            JSONObject result = sender.sendRequest(LsRequest, MASTER, MASTERPORT);
            if (result.has(LSLIST)) {
                JSONArray list = result.getJSONArray(LSLIST);
                System.out.println("File is stored at: " + list.toString());
            } else {
                System.out.println("No such files");
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

    }
}

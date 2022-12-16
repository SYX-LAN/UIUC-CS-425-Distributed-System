import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.net.Socket;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static utils.Constant.*;
import static utils.Constant.FILENAME;


public class SDFSSender {
    public JSONObject sendRequest(JSONObject request, String dest, int port) {
        Socket socket = null;
        InputStreamReader isr;
        OutputStreamWriter osw;
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        try {
            socket =new Socket(dest, port);
            isr = new InputStreamReader(socket.getInputStream());
            bufferedReader = new BufferedReader(isr);
            osw = new OutputStreamWriter(socket.getOutputStream());
            bufferedWriter = new BufferedWriter(osw);
            bufferedWriter.write(request.toString() + "\n");
            bufferedWriter.flush();
            String s = bufferedReader.readLine();
            if (s == null) {
                return new JSONObject();
            }
            JSONObject receivedJSON = new JSONObject(s);
            //System.out.println("======SENDER Receive Msg======");
            return receivedJSON;
        } catch (IOException e) {
            System.out.println(dest + " Target machine already failed");
        } finally {
            try {
                if (bufferedWriter != null) bufferedWriter.close();
                if (bufferedReader != null) bufferedReader.close();
                if (socket != null) socket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        return new JSONObject();
    }

    public void getFile(String localFileName, String SDFSFileName, String des,
                        int port, boolean version, int numVersions,
                        int actualNumVersions, long cur) throws IOException {

        // If user is getting different versions of files
        if (version) {
            System.out.println("======GET VERSIONS: " + SDFSFileName + "======");
            for (int i = 0; i < numVersions && actualNumVersions - i >= 1; ++i) {
                new Thread(new getVersions(localFileName, SDFSFileName,
                        des, port, actualNumVersions - i, cur)).start();
            }
            return;
        }

        Socket socket = null;
        DataInputStream dis = null;
        DataOutputStream localFileDos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bufferedWriter = null;

        try {
            socket = new Socket(des, port);
            localFileDos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File("").getAbsolutePath() + "/local_files/" + localFileName, false)));
            osw = new OutputStreamWriter(socket.getOutputStream());
            bufferedWriter = new BufferedWriter(osw);

            // Send the requested file name to the file holder
            JSONObject getFileRequest = new JSONObject();
            getFileRequest.put(TYPE, GETFILE);
            getFileRequest.put(FILENAME, SDFSFileName);
            bufferedWriter.write(getFileRequest.toString() + "\n");
            bufferedWriter.flush();

            // Receive the file and store the file to designated directory
            File file = new File(new File("").getAbsolutePath() + "/local_files/" + localFileName);
            // Create file if not exist
            if (!file.exists()) {
                file.createNewFile();
            }
            dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            int bufferedSize = 1024 * 10;
            byte[] buffer = new byte[bufferedSize];
            int current = 0;
            while((current = dis.read(buffer))!=-1){
                localFileDos.write(buffer, 0, current);
            }
            localFileDos.flush();
            System.out.println("======GET FILE: " + SDFSFileName + "======");
            System.out.println("======GET FILE: " + (System.currentTimeMillis() - cur) + "ms ======");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class getVersions implements Runnable {
        private Socket socket = null;
        private String localFileName;
        private String SDFSFileName;
        private int versionNum;
        private long reqTime = 0L;
        public getVersions(String localFileName, String SDFSFileName,
                           String des, int port, int versionNum, long cur) throws IOException {
            socket = new Socket(des, port);
            this.localFileName = localFileName;
            this.SDFSFileName = SDFSFileName;
            this.versionNum = versionNum;
            this.reqTime = cur;
        }

        @Override
        public void run() {
            DataInputStream dis = null;
            DataOutputStream localFileDos = null;
            OutputStreamWriter osw = null;
            BufferedWriter bufferedWriter = null;

            try {
                localFileDos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File("").getAbsolutePath() + "/local_files/" + localFileName + "_" + versionNum, false)));
                osw = new OutputStreamWriter(socket.getOutputStream());
                bufferedWriter = new BufferedWriter(osw);

                // Send the requested file name and version number to the file holder
                JSONObject getVersionsRequest = new JSONObject();
                getVersionsRequest.put(TYPE, GET_VERSION);
                getVersionsRequest.put(FILENAME, SDFSFileName);
                getVersionsRequest.put(VERSION_NUM, versionNum);
                bufferedWriter.write(getVersionsRequest.toString() + "\n");
                bufferedWriter.flush();

                // Receive the file and store the file with given version to designated directory
                File file = new File(new File("").getAbsolutePath() + "/local_files/" + localFileName + "_" + versionNum);
                // Create file if not exist
                if (!file.exists()) {
                    file.createNewFile();
                }
                dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                int bufferedSize = 1024 * 10;
                byte[] buffer = new byte[bufferedSize];
                int current = 0;
                while((current = dis.read(buffer)) != -1){
                    localFileDos.write(buffer, 0, current);
                }
                localFileDos.flush();
                System.out.println("======GET Versions: " + (System.currentTimeMillis() - reqTime) + "ms ======");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void sendFile(JSONArray hosts, String localFile,
                         String sdfsFile, int latestVersion, long cur) throws IOException {
        int size = hosts.length();
        String path = new File("").getAbsolutePath() + "/local_files/" + localFile;
        File file = new File(path);
        if (file.isDirectory()) {
            tryZipDict(path);
        }
        for (int i = 0; i < size; i++) {
            System.out.println("send file to:" + hosts.get(i));
            new Thread(new FileSender(hosts.getString(i),RESPONDERRPORT,
                    localFile, sdfsFile, latestVersion, cur)).start();
        }
    }

    private class FileSender implements Runnable {

        private Socket socket = null;
        private String localFile;
        private String sdfsFile;
        private int latestVersion;

        private long reqTime = 0L;

        String dest;
        public FileSender(String dest, int port, String localFile,
                          String sdfsFile, int latestVersion, long cur) throws IOException {
            socket = new Socket(dest, port);
            this.dest = dest;
            this.localFile = localFile;
            this.sdfsFile = sdfsFile;
            this.latestVersion = latestVersion;
            this.reqTime = cur;
        }
        @Override
        public void run() {
            try {
                JSONObject putFileRequest = new JSONObject();
                putFileRequest.put(TYPE, PUTFILE);
                putFileRequest.put(SDFSFILE, sdfsFile);
                putFileRequest.put(LATEST_VERSION, latestVersion);

                byte[] buffer = new byte[4*1024];
                int bytes;
                String path = new File("").getAbsolutePath() + "/local_files/" + localFile;
                File file = new File(path);
                if (file.isDirectory()) {
                    file = new File(path + ".zip");
                    putFileRequest.put(FILE_TYPE, DICT);
                } else {
                    putFileRequest.put(FILE_TYPE, FILE);
                }
                FileInputStream fileInputStream = new FileInputStream(file);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                putFileRequest.put(FILE_SIZE, file.length());
                putFileRequest.put(REQ_TIME, reqTime);
                int idx = 0;
                for (byte b : putFileRequest.toString().getBytes()) {
                    buffer[idx] = b;
                    idx++;
                }
                dataOutputStream.write(buffer, 0, 1024);
                dataOutputStream.flush();
                byte[] bufferFile = new byte[4 * 1024];
                while ((bytes = fileInputStream.read(bufferFile)) != -1) {
                    dataOutputStream.write(bufferFile,0, bytes);
                    dataOutputStream.flush();
                }
                fileInputStream.close();
                dataOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private void tryZipDict(String path) {
        File zipFile = new File(path + ".zip");
        File tobeZipped = new File(path);
        System.out.println("tryZipDict:" + zipFile.getName());
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
//        return new File(path + ".zip");
    }
}
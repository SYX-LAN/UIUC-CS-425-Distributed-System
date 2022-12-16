import org.json.JSONObject;
import utils.Constant;

import java.net.InetAddress;

import static utils.Constant.*;
import static utils.Constant.MASTERPORT;

public class InferenceHandler implements Runnable{

    String inputFile;
    String model;
    int batchSize;
    private SDFSSender sender;

    public InferenceHandler(String inputFile, String model, int batchSize) {
        this.inputFile = inputFile;
        this.model = model;
        this.batchSize = batchSize;
        sender = new SDFSSender();
    }

    public void run() {
        // Send inference request to master server to get the job assigned
        JSONObject getRequest = new JSONObject();
        getRequest.put(TYPE, INFERENCE_REQUEST);
        getRequest.put(FILENAME, inputFile);
        getRequest.put(MODEL, model);
        getRequest.put(BATCH_SIZE, batchSize);
        System.out.println("============Job Submitted==============");
        JSONObject receivedJSON = sender.sendRequest(getRequest, MASTER, MASTERPORT);
        if (!receivedJSON.has(INFERENCE_SUCCESS)) {
            System.out.println("============InferenceHandler MASTER FAILED============");
            receivedJSON = sender.sendRequest(getRequest, GROUP[1], MASTERPORT);
            if (receivedJSON.has(INFERENCE_SUCCESS) && receivedJSON.getBoolean(INFERENCE_SUCCESS)) {
                System.out.println("===========New BACKUP Job Finished===========");
            } else {
                System.out.println("============New BACKUP Job Failed============");
            }
        } else if (receivedJSON.getBoolean(INFERENCE_SUCCESS)) {
            System.out.println("===========Job Finished===========");
        } else {
            System.out.println("============Job Failed============");
        }
    }
}

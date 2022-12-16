import org.json.JSONArray;
import org.json.JSONObject;

import static utils.Constant.*;

public class CommandHandler {

    private SDFSSender sender;

    public CommandHandler() {
        sender = new SDFSSender();
    }

    public void C1(String jobID, boolean isBoth) {
        if (!isBoth) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(BOTH_HANDLED, false);
            jsonObject.put(TYPE, C1_QUERY_RATE);
            jsonObject.put(JOB_ID, jobID);
            JSONObject receivedJSON = sender.sendRequest(jsonObject, MASTER, MASTERPORT);
            System.out.println("========The current query rate of job_" + jobID + ": " + receivedJSON.getDouble(QUERY_RATE) + " (queries/second)========");
            System.out.println("========The number of queries processed so far of job_" + jobID + ": " + receivedJSON.getInt(TOTAL_QUERIES_PROCESSED) + "========");
        } else {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(BOTH_HANDLED, true);
            jsonObject.put(TYPE, C1_QUERY_RATE);
            JSONObject receivedJSON = sender.sendRequest(jsonObject, MASTER, MASTERPORT);
            double queryRateJob1 = receivedJSON.getDouble(QUERY_RATE_JOB1);
            double queryRateJob2 = receivedJSON.getDouble(QUERY_RATE_JOB2);
            System.out.println("========The current query rate of job_1" + ": " + queryRateJob1 + " (queries/second)========");
            System.out.println("========The number of queries processed so far of job_1" + ": " + receivedJSON.getInt(TOTAL_QUERIES_PROCESSED_JOB1) + "========");
            System.out.println("\n");
            System.out.println("========The current query rate of job_2" + ": " + queryRateJob2 + " (queries/second)========");
            System.out.println("========The number of queries processed so far of job_2" + ": " + receivedJSON.getInt(TOTAL_QUERIES_PROCESSED_JOB2) + "========");
            System.out.println("\n");
            if (queryRateJob1 >= queryRateJob2) {
                System.out.println("========Job_1 is faster than Job_2 by " + String.format("%.2f", 100 * ((queryRateJob1 - queryRateJob2) / queryRateJob1)) + "%==========");
            } else {
                System.out.println("========Job_2 is faster than Job_1 by " + String.format("%.2f",100 * ((queryRateJob2 - queryRateJob1) / queryRateJob2)) + "%==========");
            }
        }
    }

    public void C4(String jobID) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(TYPE, C4_QUERY_RESULT);
        jsonObject.put(JOB_ID, jobID);
        JSONObject receivedJSON = sender.sendRequest(jsonObject, MASTER, MASTERPORT);
        System.out.println("=========The inference results of Job_" + jobID + " is: ==========");
        System.out.println(receivedJSON.getString(INFERENCE_CONTENTS));
    }

    public void C5() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(TYPE, C5_VM_ASSIGN);
        JSONObject receivedJSON = sender.sendRequest(jsonObject, MASTER, MASTERPORT);
        if (receivedJSON.getInt(NUM_JOBS) == 1) {
            JSONArray jsonArray = receivedJSON.getJSONArray(VM_ASSIGNED);
            String vm = "";
            for (int i = 0; i != jsonArray.length(); ++i) {
                vm += jsonArray.getString(i);
                vm += "\n";
            }
            String jobID = receivedJSON.getString(JOB_ID);
            if (jobID.equals("1")) {
                System.out.println("=======Current set of VMs assigned to Job_1========");
            } else if (jobID.equals("2")) {
                System.out.println("=======Current set of VMs assigned to Job_2========");
            }
            System.out.println(vm);
        } else if (receivedJSON.getInt(NUM_JOBS) == 2) {
            JSONArray jsonArray1 = receivedJSON.getJSONArray(VM_ASSIGNED_JOB1);
            JSONArray jsonArray2 = receivedJSON.getJSONArray(VM_ASSIGNED_JOB2);
            String vm1 = "";
            String vm2 = "";
            for (int i = 0; i != jsonArray1.length(); ++i) {
                vm1 += jsonArray1.getString(i);
                vm1 += "\n";
            }
            for (int i = 0; i != jsonArray2.length(); ++i) {
                vm2 += jsonArray2.getString(i);
                vm2 += "\n";
            }
            System.out.println("=======Current set of VMs assigned to Job_1========");
            System.out.println(vm1 + "\n");
            System.out.println("=======Current set of VMs assigned to Job_2========");
            System.out.println(vm2);
        }
    }

    public void C2(String jobID) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(TYPE, C2_STATISTICS);
        jsonObject.put(JOB_ID, jobID);
        JSONObject receivedJSON = sender.sendRequest(jsonObject, MASTER, MASTERPORT);
        double average = receivedJSON.getDouble(AVERAGE);
        double std = receivedJSON.getDouble(STD);
        double median = receivedJSON.getDouble(MEDIAN);
        long ninetyPercentile = receivedJSON.getLong(NINETY_PERCENTILE);
        long ninetyFivePercentile = receivedJSON.getLong(NINETY_FIVE_PERCENTILE);
        long ninetyNinePercentile = receivedJSON.getLong(NINETY_NINE_PERCENTILE);
        System.out.println("=========Processing time statistics of Job_" + jobID + ": ===========\n");
        System.out.println("Average: " + average + " ms");
        System.out.println("Standard deviation: " + std + " ms");
        System.out.println("Median: " + median + " ms");
        System.out.println("90th percentile: " + ninetyPercentile + " ms");
        System.out.println("95th percentile: " + ninetyFivePercentile + " ms");
        System.out.println("99th percentile: " + ninetyNinePercentile + " ms");
    }

    public void C3(String model, int batchSize, int implicitBatchSizeNumber) {
        JSONObject jsonObject = new JSONObject();
        if (model.equals("number")) {
            jsonObject.put(TYPE, C3_SETUP);
            jsonObject.put(MODEL, model);
            jsonObject.put(BATCH_SIZE, batchSize);
            for (int i = 0; i < 2; ++i) {
                JSONObject receivedJSON = sender.sendRequest(jsonObject, GROUP[i], RESPONDERRPORT);
            }

        } else if (model.equals("weather")) {
            jsonObject.put(TYPE, C3_SETUP);
            jsonObject.put(MODEL, model);
            jsonObject.put(BATCH_SIZE, batchSize);
            jsonObject.put(IMPLICIT_BATCH_SIZE_NUMBER, implicitBatchSizeNumber);
            sender.sendRequest(jsonObject, GROUP[0], RESPONDERRPORT);
//            for (int i = 0; i < 2; ++i) {
////                JSONObject receivedJSON = sender.sendRequest(jsonObject, GROUP[i], RESPONDERRPORT);
//            }
        }

    }
}

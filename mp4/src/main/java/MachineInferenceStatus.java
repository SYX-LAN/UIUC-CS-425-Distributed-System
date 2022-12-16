public class MachineInferenceStatus {
    private String jobID;
    private String model;
    private String inputFile;
    int idx;
    int batchSize;
    private boolean available;

    public MachineInferenceStatus(String jobID, String model, String inputFile, int idx, int batchSize, boolean available) {
        this.jobID = jobID;
        this.model = model;
        this.inputFile = inputFile;
        this.idx = idx;
        this.batchSize = batchSize;
        this.available = available;
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }
}

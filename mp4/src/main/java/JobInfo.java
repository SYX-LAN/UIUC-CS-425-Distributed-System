public class JobInfo {
    private String inputFile;
    private String jobID;
    private String model;
    private int machineNum;
    private int totalMachine;

    public JobInfo(String inputFile, String jobID, String model, int machineNum, int totalMachine) {
        this.inputFile = inputFile;
        this.jobID = jobID;
        this.model = model;
        this.machineNum = machineNum;
        this.totalMachine = totalMachine;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
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

    public int getMachineNum() {
        return machineNum;
    }

    public void setMachineNum(int machineNum) {
        this.machineNum = machineNum;
    }

    public int getTotalMachine() {
        return totalMachine;
    }

    public void setTotalMachine(int totalMachine) {
        this.totalMachine = totalMachine;
    }
}

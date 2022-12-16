public class JobTaker {
    private String hostname;
    int machineNumber;
    int totalMachine;

    String model;
    String inputFile;
    Boolean finished;

    public Boolean getFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    public JobTaker(String hostname, int machineNumber, int totalMachine, String model, String inputFile, Boolean finished) {
        this.hostname = hostname;
        this.machineNumber = machineNumber;
        this.totalMachine = totalMachine;
        this.model = model;
        this.inputFile = inputFile;
        this.finished = finished;
    }

    public String getHostname() {
        return hostname;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getMachineNumber() {
        return machineNumber;
    }

    public void setMachineNumber(int machineNumber) {
        this.machineNumber = machineNumber;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public int getTotalMachine() {
        return totalMachine;
    }

    public void setTotalMachine(int totalMachine) {
        this.totalMachine = totalMachine;
    }
}

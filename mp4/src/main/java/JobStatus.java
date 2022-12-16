import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JobStatus {
    String inputFile;
    String model;
    Set<Integer> processed;
    int batchSize;
    int totalBatchSize;
    List<Long> queryProcessingTimes;

    public JobStatus(String inputFile, String model, int batchSize, int totalBatchSize) {
        this.inputFile = inputFile;
        this.model = model;
        processed = new HashSet<>();
        queryProcessingTimes = new ArrayList<>();
        this.batchSize = batchSize;
        this.totalBatchSize = totalBatchSize;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getTotalBatchSize() {
        return totalBatchSize;
    }

    public void setTotalBatchSize(int totalBatchSize) {
        this.totalBatchSize = totalBatchSize;
    }

    public Set<Integer> getProcessed() {
        return processed;
    }

    public void setProcessed(Set<Integer> processed) {
        this.processed = processed;
    }

    public List<Long> getQueryProcessingTimes() {
        return queryProcessingTimes;
    }

    public void setQueryProcessingTimes(List<Long> queryProcessingTimes) {
        this.queryProcessingTimes = queryProcessingTimes;
    }
}

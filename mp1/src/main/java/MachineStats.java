import java.time.Duration;

/*
 * MachineStats class represents status and statistics of each machine
 */
public class MachineStats {

    private int status;
    private int lineCount;
    private Duration duration;

    public MachineStats(int status, int lineCount, Duration duration) {
        this.status = status;
        this.lineCount = lineCount;
        this.duration = duration;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }
}

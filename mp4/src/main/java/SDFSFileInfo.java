import java.sql.Timestamp;

public class SDFSFileInfo {

    private Timestamp lastUpdatedTime;

    private int latestVersion;


    public SDFSFileInfo(Timestamp ts, int latestVersion) {
        this.lastUpdatedTime = ts;
        this.latestVersion = latestVersion;
    }

    public Timestamp getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Timestamp lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public int getLatestVersion() {
        return latestVersion;
    }

    public void setLatestVersion(int latestVersion) {
        this.latestVersion = latestVersion;
    }
}

public class MemberStatus {
    private Boolean isAlive;
    private String id;

    public MemberStatus(Boolean isAlive, String id) {
        this.isAlive = isAlive;
        this.id = id;
    }

    public Boolean getAlive() {
        return isAlive;
    }

    public void setAlive(Boolean alive) {
        isAlive = alive;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}

import org.unix4j.unix.grep.GrepOptionSet_Fcilnvx;

// GrepStruct class represents query details
public class GrepStruct implements java.io.Serializable{
    private String option;
    private String regex;
    public GrepStruct(String option, String regex) {
        this.option = option;
        this.regex = regex;
    }

    public String  getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }
}
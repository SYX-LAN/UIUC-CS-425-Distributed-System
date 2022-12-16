import org.unix4j.Unix4j;
import org.unix4j.unix.Grep;
import org.unix4j.unix.grep.GrepOptionSet_Fcilnvx;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

public class LogSearch {
    private static final Map<String, GrepOptionSet_Fcilnvx> optionMap;
    static {
        optionMap = new HashMap<>();
        optionMap.put("-c", Grep.Options.c);
        optionMap.put("-F", Grep.Options.F);
        optionMap.put("-i", Grep.Options.i);
        optionMap.put("-l", Grep.Options.l);
        optionMap.put("-v", Grep.Options.v);
        optionMap.put("-x", Grep.Options.x);
        optionMap.put("-n", Grep.Options.n);
        optionMap.put("-Ec", Grep.Options.c);
    }
    public static List<String> search(GrepStruct grep) {
        String regex = grep.getRegex();
        String option = grep.getOption();
        File f = matchLog();
        // can not find log file, result is null
        if (f == null) {
            return null;
        }
        // use Unix4j to implement grep
        List<String> result;
        if (option.equals("-c")) {
            List<String> temp = Unix4j.grep(Grep.Options.F, regex, f).toStringList();
            result = new ArrayList<>();
            result.add(temp.size() + "");
        } else {
            result = Unix4j.grep(optionMap.get(option), regex, f).toStringList();
        }
        // add log name at first index
        result.add(0, f.getName() + ":");
        return result;
    }

    private static File matchLog() {
        // get current file path
        String source = new File("").getAbsolutePath();
        // find all files in this path
        File[] dir = new File(source).listFiles();
        if (dir == null || dir.length == 0) {
            return null;
        }

        for (File subFile : dir) {
            // find log file and return
            if (subFile.isFile() && subFile.getName().endsWith(".log")) {
                return subFile;
            }
        }

        return null;
    }
}

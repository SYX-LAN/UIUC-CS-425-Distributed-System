import java.io.File;

public class DeleteFilesInDir {

    public void deleteFiles(String dir) {
        File file = new File(dir);
        deleteFile(file);
    }
    public static void deleteFile(File file){
        if (file == null || !file.exists()){
            return;
        }
        File[] files = file.listFiles();
        for (File f: files){
            if (f.isDirectory()){
                deleteFile(f);
                f.delete();
            }else {
                f.delete();
            }
        }
    }
}

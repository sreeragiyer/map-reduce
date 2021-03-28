import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public abstract class Mapper {
    protected abstract HashMap<String, String> map(String k, String v);

    public String execute(String docId, String inputText) {
        HashMap<String, String> output = map(docId, inputText);
        String currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String tempFileName = "intermediate_" + currentTime + ".txt";

        File tempFile = new File(tempFileName);
        try {
            tempFile.createNewFile();
            FileWriter fw = new FileWriter(tempFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            output.entrySet().forEach(entry -> {
                try {
                    bw.write(entry.getKey() + ":" + entry.getValue());
                } catch (IOException e) {
                    //TODO : Handle the exception with proper messaging.
                }
            });
            bw.close();
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }
        return tempFileName;
    }
}

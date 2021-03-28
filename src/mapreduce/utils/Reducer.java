package mapreduce.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public abstract class Reducer {

    protected abstract List<String> reduce(String key, List<String> values);

    public void execute(String outputFileLocation, HashMap<String, List<String>> reducerInput) {
        File outputFile = new File(outputFileLocation);
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
            FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            reducerInput.entrySet().forEach(entry -> {
                List<String> reduceOutput = reduce(entry.getKey(), entry.getValue());
                try {
                    bw.write(entry.getKey() + " : " + reduceOutput.toString() + "\n");
                } catch (IOException e) {
                    // TODO : Add proper exception messaging.
                }
            });
            bw.close();
        } catch (IOException e) {
            // TODO : Add proper exception messaging.
        }

    }
}

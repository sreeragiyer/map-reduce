package mapreduce.utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

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

    private HashMap<String, List<String>> getMapFromTextFile(String reduceDirPath) {
        File dir = new File(reduceDirPath);
        HashMap<String, List<String>> map = new HashMap<>();
        for (File file: dir.listFiles()) {
            try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()))) {
                stream.forEach(line-> {
                    String[] parts = line.split(":");
                    map.computeIfAbsent(parts[0], k -> new ArrayList<>());
                    map.get(parts[0]).add(parts[1]);
                });
            } catch (IOException e) {
                //TODO : Handle the exception with proper messaging.
            }
        }
        return map;
    }

    public static void main(String[] args) {
        try {

            String opFileLoc = args[0];
            String reduceDirPath = args[1];
            String className = args[2];
            Reducer obj = (Reducer) Class.forName(className).getDeclaredConstructor().newInstance();
            HashMap<String, List<String>> tempData = obj.getMapFromTextFile(reduceDirPath);
            obj.execute(opFileLoc, tempData);
        }
        catch(Exception e) {
            System.out.println("class not found");
        }
    }
}

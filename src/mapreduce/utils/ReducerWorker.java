package mapreduce.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

/**
 * The ReducerWorker class that defines the working of a worker node acting as a reducer
 */
public class ReducerWorker {

    // method to call user defined reduce function and write output to N files
    public void execute(Reducer obj, String outputFileLocation, HashMap<String, List<String>> reducerInput) {
        // create output file if it does not exist
        File outputFile = new File(outputFileLocation);
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
            FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            // perform reduce operation for each key
            reducerInput.entrySet().forEach(entry -> {
                List<String> reduceOutput = null;
                try {
                    reduceOutput = obj.reduce(entry.getKey(), entry.getValue());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                try {
                    // write to output file
                    bw.write(entry.getKey() + " : " + reduceOutput.toString() + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // method to get all keys abd their corresponding values
    private HashMap<String, List<String>> getMapFromTextFile(String reduceDirPath) {
        File dir = new File(reduceDirPath);
        HashMap<String, List<String>> map = new HashMap<>();
        // all values for a given key will be in the same file
        for (File file: dir.listFiles()) {
            try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()))) {
                stream.forEach(line-> {
                    String[] parts = line.split(":");
                    map.computeIfAbsent(parts[0], k -> new ArrayList<>());
                    map.get(parts[0]).add(parts[1]);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    // will be invoked by the master as a separate process
    public static void main(String[] args) throws Exception{
        // read all input parameters provided by master
        String opFileLoc = args[0];
        String reduceDirPath = args[1];
        String reducerKey = args[2];
        int timeout = Integer.parseInt(args[3]);
        timeout = timeout > 0 ? timeout : 6000;

        while (args.length > 4 && reduceDirPath.equalsIgnoreCase("reducer_" + args[4])); // to simulate fault tolerance
        MapReduce mr = new MapReduce();
        Reducer obj = null;
        try {
            obj = mr.getReducerObj(reducerKey); // get from RMI registry
        } catch (RemoteException | NotBoundException | MalformedURLException e) {
            e.printStackTrace();
            throw e;
        }
        ReducerWorker r = new ReducerWorker();
        HashMap<String, List<String>> tempData = r.getMapFromTextFile(reduceDirPath);
        r.execute(obj, opFileLoc, tempData);
        
    }
}

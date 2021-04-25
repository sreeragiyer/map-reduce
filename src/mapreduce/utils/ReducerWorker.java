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
import java.util.Locale;
import java.util.stream.Stream;

public class ReducerWorker {
    public void execute(Reducer obj, String outputFileLocation, HashMap<String, List<String>> reducerInput) {
        File outputFile = new File(outputFileLocation);
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
            FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            reducerInput.entrySet().forEach(entry -> {
                List<String> reduceOutput = null;
                try {
                    reduceOutput = obj.reduce(entry.getKey(), entry.getValue());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
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

    private static int faultId = -1;
    private static boolean faultFlag = false;
    private static void simulateFault(String reducerId, int num_processes) {
        // picks a random integer between 0 and 2*num_processes,
        // if it is equal to the mapper id, then this mapper will be faulty (infinite loop)
        if(faultId == -1 && num_processes>0) {
            faultId = (int) (Math.random() * num_processes);
        }
        if(!faultFlag && "reducer_"+faultId == reducerId.toLowerCase(Locale.ROOT)) {
            faultFlag = true;
            while (true) ;
        }
    }

    public static void main(String[] args) {
        try {
            String opFileLoc = args[0];
            String reduceDirPath = args[1];
            String reducerKey = args[2];
            String num_processes = args[3];
            Runnable task = () -> {
                // simulateFault(reduceDirPath, Integer.parseInt(num_processes));
                MapReduce mr = new MapReduce();
                Reducer obj = null;
                try {
                    obj = mr.getReducerObj(reducerKey);
                }
                catch (RemoteException | NotBoundException | MalformedURLException e) {
                    e.printStackTrace();
                    System.exit(2);
                }
                ReducerWorker r = new ReducerWorker();
                HashMap<String, List<String>> tempData = r.getMapFromTextFile(reduceDirPath);
                r.execute(obj, opFileLoc, tempData);
            };
            Thread reduceThread = new Thread(task);
            reduceThread.start();
            reduceThread.join(6000);
            if(reduceThread.isAlive()) {
                System.exit(1);
            }

        }
        catch(Exception e) {
            System.out.println("class not found");
        }
    }
}

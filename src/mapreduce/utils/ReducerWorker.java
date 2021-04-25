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

    public static void main(String[] args) {
        try {
            String opFileLoc = args[0];
            String reduceDirPath = args[1];
            String reducerKey = args[2];
            int timeout = Integer.parseInt(args[3]);
            timeout = timeout > 0 ? timeout : 6000;

            Runnable task = () -> {
                while(args.length>4 && reduceDirPath.equalsIgnoreCase("reducer_"+args[4]));
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
            reduceThread.join(timeout);
            if(reduceThread.isAlive()) {
                System.exit(1);
            }

        }
        catch(Exception e) {
            System.out.println("class not found");
        }
    }
}

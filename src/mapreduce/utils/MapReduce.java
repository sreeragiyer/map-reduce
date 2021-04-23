package mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * The main mapReduce class which contains mapreduce function that executes the map reduce code.
 */
public class MapReduce {

    String mapperKey;
    String reducerKey;

    public void mapReduce(MapReduceSpecification specs) {
        addToRegistry(specs.mapper, specs.mapperKey);
        addToRegistry(specs.reducer, specs.reducerKey);
        String inputFileLocation = specs.inputFileLocation;
        int lineCount = getLineCount(inputFileLocation);
        List<Process> map_processes = new ArrayList<>();
        List<Process> reducer_processes = new ArrayList<>();
        int num_processes = specs.numProcesses;
        int partition_length = lineCount/(specs.numProcesses);
        try {
            for (int i=0; i<num_processes; i++) {
                int start_line = i*partition_length;
                int end_line = start_line+partition_length-1;
                if (end_line>=lineCount) {
                    end_line = lineCount-1;
                }
                System.out.println(specs.mapperKey + "-" + start_line + "-" + end_line);
                ProcessBuilder pbMapper = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/MapperWorker", specs.mapperKey,
                        inputFileLocation, String.valueOf(start_line), String.valueOf(end_line),
                        String.valueOf(num_processes), String.valueOf(i));

                Process mapperProcess = pbMapper.inheritIO().start();
                map_processes.add(mapperProcess);
            }
            for (int i=0; i<num_processes; i++) {
                map_processes.get(i).waitFor();
            }

            for (int i=0; i<num_processes; i++) {
                String outputFilePath = specs.outputFileLocation + "/partition_" + i + ".txt";
                ProcessBuilder pbReducer = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/ReducerWorker",
                        outputFilePath, "reducer_" + i, specs.reducerKey);
                Process reducerProcess = pbReducer.inheritIO().start();
                reducer_processes.add(reducerProcess);
            }
            for (int i=0; i<num_processes; i++) {
                reducer_processes.get(i).waitFor();
            }
            System.out.println("Completed Execution");
            for (int i=0; i<num_processes; i++) {
                File dir = new File("reducer_" + i);
                for (File file: dir.listFiles())
                    if (!file.isDirectory())
                        file.delete();
                dir.delete();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int getLineCount(String inputFileLocation) {
        int line_count = 0 ;
        try (Stream<String> lines = Files.lines(Paths.get(inputFileLocation))) {
            line_count =  (int) lines.count();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line_count;
    }

    public void addToRegistry(Mapper obj, String objKey) {
        try {
            mapperKey = objKey;

            // create the RMI registry if it doesn't already exist
            try {
                LocateRegistry.createRegistry(1099);
            }
            catch(ExportException ee) {
                System.out.println("RMI registry already running on port 1099!");
            }

            // get reference to the registry and bind
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(objKey, obj);
        }
        catch(RemoteException | AlreadyBoundException ex) {
            ex.printStackTrace();
        }
    }

    public void addToRegistry(Reducer obj, String objKey) {
        try {
            reducerKey = objKey;

            // get reference to the registry and bind
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(objKey, obj);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public Mapper getMapperObj(String objKey) throws Exception {
        try {
            // fetch user defined mapper object from registry
            Mapper obj = (Mapper) Naming.lookup("rmi://localhost/"+objKey);
            return obj;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    public Reducer getReducerObj(String objKey) throws Exception {
        try {
            // fetch user defined mapper object from registry
            Reducer obj = (Reducer) Naming.lookup("rmi://localhost/"+objKey);
            return obj;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

}

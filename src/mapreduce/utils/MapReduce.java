package mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * The main mapReduce class which contains mapreduce function that executes the map reduce code.
 * code for Master
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

        // pick one mapper and one reducer randomly as faulty
        int faultyMapper = getFaultyWorker(num_processes);
        int faultyReducer = getFaultyWorker(num_processes);

        try {
            for (int i=0; i<num_processes; i++) {
                // start all mapper processes
                int start_line = i*partition_length;
                int end_line = start_line+partition_length-1;
                if (end_line>=lineCount) {
                    end_line = lineCount-1;
                }
                System.out.println(specs.mapperKey + "-"+i+" operating from line " + start_line + " to line " + end_line);
                ProcessBuilder pbMapper = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/MapperWorker", specs.mapperKey,
                        inputFileLocation, String.valueOf(start_line), String.valueOf(end_line),
                        String.valueOf(num_processes), String.valueOf(i), String.valueOf(specs.timeout), String.valueOf(faultyMapper));

                Process mapperProcess = pbMapper.inheritIO().start();
                map_processes.add(mapperProcess);
            }

            // wait for all mapper processes to complete
            for (int i=0; i<map_processes.size(); i++) {
                boolean isTimedOut = false;
                if(!map_processes.get(i).waitFor(specs.timeout, TimeUnit.MILLISECONDS)) {
                    map_processes.get(i).destroy();
                    isTimedOut = true;
                }
                System.out.println(specs.mapperKey + "-"+(i>=num_processes? faultyMapper : i)+(isTimedOut || map_processes.get(i).exitValue() != 0 ? " failed! Retrying..." : " finished successfully."));
                if(i<num_processes && (isTimedOut || map_processes.get(i).exitValue() != 0)) {
                    // if there was a fault in any of the n (num_processes) original workers,
                    // then restart that worker
                    int start_line = i*partition_length;
                    int end_line = start_line+partition_length-1;
                    if (end_line>=lineCount) {
                        end_line = lineCount-1;
                    }
                    ProcessBuilder pbMapper = new ProcessBuilder("java",
                            "-cp", "./src", "mapreduce/utils/MapperWorker", specs.mapperKey,
                            inputFileLocation, String.valueOf(start_line), String.valueOf(end_line),
                            String.valueOf(num_processes), String.valueOf(i), String.valueOf(specs.timeout));
                    Process mapperProcess = pbMapper.inheritIO().start();
                    map_processes.add(mapperProcess);
                }
            }

            // all mapper processes are now completed
            for (int i=0; i<num_processes; i++) {
                // start all reducer processes
                String outputFilePath = specs.outputFileLocation + "/partition_" + i + ".txt";
                ProcessBuilder pbReducer = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/ReducerWorker",
                        outputFilePath, "reducer_" + i, specs.reducerKey, String.valueOf(specs.timeout), String.valueOf(faultyReducer));
                Process reducerProcess = pbReducer.inheritIO().start();
                reducer_processes.add(reducerProcess);
            }

            // wait for all reducer processes to complete
            for (int i=0; i<reducer_processes.size(); i++) {
                boolean isTimedOut = false;
                if(!reducer_processes.get(i).waitFor(specs.timeout, TimeUnit.MILLISECONDS)) {
                    reducer_processes.get(i).destroy();
                    isTimedOut = true;
                }
                System.out.println(specs.reducerKey + "-"+(i>=num_processes? faultyReducer : i)+(isTimedOut || reducer_processes.get(i).exitValue() != 0 ? " failed! Retrying..." : " finished successfully."));
                if(i<num_processes && (isTimedOut || reducer_processes.get(i).exitValue() != 0)) {
                    // if there was a fault in any of the n (num_processes) original workers,
                    // then restart that worker
                    String outputFilePath = specs.outputFileLocation + "/partition_" + i + ".txt";
                    ProcessBuilder pbReducer = new ProcessBuilder("java",
                            "-cp", "./src", "mapreduce/utils/ReducerWorker",
                            outputFilePath, "reducer_" + i, specs.reducerKey, String.valueOf(specs.timeout));
                    Process reducerProcess = pbReducer.inheritIO().start();
                    reducer_processes.add(reducerProcess);
                }
            }
            System.out.println("Map Reduce completed for "+specs.mapperKey+"/"+specs.reducerKey+"\n");
            for (int i=0; i<num_processes; i++) {
                // delete intermediate files (since there are N*N files formed)
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

    // return a random integer [0, N-1] to simulate fault tolerance
    private int getFaultyWorker(int num_processes) {
        return  (int) (Math.random() * num_processes);
    }

    // counts number of lines in the filw
    private int getLineCount(String inputFileLocation) {
        int line_count = 0 ;
        try (Stream<String> lines = Files.lines(Paths.get(inputFileLocation))) {
            line_count =  (int) lines.count();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line_count;
    }

    // adds mapper object to RMI registry
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

    // adds reducer obj to RMI registry
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

    // fetches user defined mapper object from RMI registry
    public Mapper getMapperObj(String objKey) throws RemoteException, NotBoundException, MalformedURLException {
        try {
            Mapper obj = (Mapper) Naming.lookup("rmi://localhost/"+objKey);
            return obj;
        }
        catch (RemoteException | NotBoundException | MalformedURLException ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    // fetches user defined mapper object from RMI registry
    public Reducer getReducerObj(String objKey) throws RemoteException, NotBoundException, MalformedURLException {
        try {
            Reducer obj = (Reducer) Naming.lookup("rmi://localhost/"+objKey);
            return obj;
        }
        catch (RemoteException | NotBoundException | MalformedURLException ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

}

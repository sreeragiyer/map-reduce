package mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * The main mapReduce class which contains mapreduce function that executes the map reduce code.
 */
public class MapReduce {

    public void mapReduce(MapReduceSpecification specs) {
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
                System.out.println(specs.mapperClassPath + "-" + start_line + "-" + end_line);
                ProcessBuilder pbMapper = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/Mapper", specs.mapperClassPath,
                        inputFileLocation, String.valueOf(start_line), String.valueOf(end_line),
                        String.valueOf(num_processes), String.valueOf(i));

                Process mapperProcess = pbMapper.start();
                map_processes.add(mapperProcess);
            }
            for (int i=0; i<num_processes; i++) {
                map_processes.get(i).waitFor();
            }

            for (int i=0; i<num_processes; i++) {
                String outputFilePath = specs.outputFileLocation + "/partition_" + i + ".txt";
                ProcessBuilder pbReducer = new ProcessBuilder("java",
                        "-cp", "./src", "mapreduce/utils/Reducer",
                        outputFilePath, "reducer_" + i, specs.reducerClassPath);
                Process reducerProcess = pbReducer.start();
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
            //TODO : Handle the exception with proper messaging.
        }
    }

    private int getLineCount(String inputFileLocation) {
        int line_count = 0 ;
        try (Stream<String> lines = Files.lines(Paths.get(inputFileLocation))) {
            line_count =  (int) lines.count();
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }
        return line_count;
    }
}

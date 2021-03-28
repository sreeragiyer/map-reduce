package mapreduce.utils;


import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The main mapReduce class which contains mapreduce function that executes the map reduce code.
 */
public class MapReduce {

    public void mapReduce(MapReduceSpecification specs) {
        String inputFileLocation = specs.inputFileLocation;
        Mapper mapper = specs.mapper;
        Reducer reducer = specs.reducer;
        try {
            String data = new String(Files.readAllBytes(Paths.get(inputFileLocation)));
            String tempFilePath = mapper.execute("", data);
            Map<String, String> tempData = getMapFromTextFile(tempFilePath);

        } catch (IOException e) {
            //Do Nothing.
        }
    }

    private Map<String, String> getMapFromTextFile(String filePath) {
        Map<String, String> map = new HashMap<>();
        BufferedReader br = null;
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(line-> {
                String[] parts = line.split(":");
                map.put(parts[0], parts[1]);
            });
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }
        return map;
    }
}

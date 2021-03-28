package mapreduce.utils;


import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
            HashMap<String, List<String>> tempData = getMapFromTextFile(tempFilePath);
            reducer.execute(specs.outputFileLocation, tempData);
            File myObj = new File(tempFilePath);
            myObj.delete();
        } catch (IOException e) {
            //Do Nothing.
        }
    }

    private HashMap<String, List<String>> getMapFromTextFile(String filePath) {
        HashMap<String, List<String>> map = new HashMap<>();
        BufferedReader br = null;
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(line-> {
                String[] parts = line.split(":");
                if (map.get(parts[0])==null) {
                    map.put(parts[0], new ArrayList<>());
                }
                map.get(parts[0]).add(parts[1]);
            });
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }
        return map;
    }
}

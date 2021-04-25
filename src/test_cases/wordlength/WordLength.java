package test_cases.wordlength;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;
import java.rmi.RemoteException;

/**
 * The main class that starts the MapReduce operation
 */
public class WordLength {

    public  static void main(String[] args) {
        try {
            // specifying all input parameters
            MapReduceSpecification mrs = new MapReduceSpecification();
            mrs.numProcesses = 5;
            mrs.inputFileLocation = System.getProperty("user.dir") + "/data/loremipsum.txt"; // test cases will be run from the root dir
            mrs.outputFileLocation = System.getProperty("user.dir") + "/test_cases_output/wordlength";
            File directory = new File(mrs.outputFileLocation);
            directory.mkdir();
            mrs.mapperKey = "WordLengthMapper";
            mrs.reducerKey = "WordLengthReducer";
            mrs.mapper = new WordLengthMapper();
            mrs.reducer = new WordLengthReducer();
            mrs.timeout = 2000; // setting a lesser timeout since this operates on smaller data
            MapReduce obj = new MapReduce();
            // start the map reduce operation
            obj.mapReduce(mrs);
            System.exit(0);  // required since mapper and reducer extend UnicastRemoteObject
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }
}

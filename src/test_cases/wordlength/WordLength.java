package test_cases.wordlength;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;
import java.rmi.RemoteException;

public class WordLength {

    public  static void main(String[] args) {
        try {
            MapReduceSpecification mrs = new MapReduceSpecification();
            mrs.numProcesses = 3;
            mrs.inputFileLocation = System.getProperty("user.dir") + "/data/loremipsum.txt"; // test cases will be run from the root dir
            mrs.outputFileLocation = System.getProperty("user.dir") + "/test_cases_output/wordlength";
            File directory = new File(mrs.outputFileLocation);
            directory.mkdir();
            mrs.mapperKey = "WordLengthMapper";
            mrs.reducerKey = "WordLengthReducer";
            mrs.mapper = new WordLengthMapper();
            mrs.reducer = new WordLengthReducer();
            mrs.timeout = 2000;
            MapReduce obj = new MapReduce();
            obj.mapReduce(mrs);
            System.exit(0);
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }
}

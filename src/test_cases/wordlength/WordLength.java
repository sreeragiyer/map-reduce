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
            mrs.inputFileLocation = System.getProperty("user.dir") + "/src/data/loremipsum.txt";
            mrs.outputFileLocation = System.getProperty("user.dir") + "/src/test_cases_output/wordlength";
            File directory = new File(mrs.outputFileLocation);
            directory.mkdir();
            mrs.mapperKey = "WordLengthMapper";
            mrs.reducerKey = "WordLengthReducer";
            mrs.mapper = new WordLengthMapper();
            mrs.reducer = new WordLengthReducer();
            MapReduce obj = new MapReduce();
            obj.mapReduce(mrs);
            System.exit(0);
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }
}

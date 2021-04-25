package test_cases.wordcount;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;
import java.rmi.RemoteException;

/**
 * The main class that starts the MapReduce operation
 */
public class WordCount {

    public static void main(String[] args)
    {
        try {
            // specifying all input parameters
            MapReduceSpecification mp = new MapReduceSpecification();
            mp.numProcesses = 4;
            mp.inputFileLocation = System.getProperty("user.dir") + "/data/hamlet.txt"; // test cases will be run from the root dir
            mp.outputFileLocation = System.getProperty("user.dir") + "/test_cases_output/wordcount";
            File directory = new File(mp.outputFileLocation);
            directory.mkdir();
            mp.mapperKey = "WordCountMapper";
            mp.reducerKey = "WordCountReducer";
            mp.mapper = new WordCountMapper();
            mp.reducer = new WordCountReducer();
            MapReduce obj = new MapReduce();
            // call map reduce library
            obj.mapReduce(mp);
            System.exit(0);             // required since mapper and reducer extend UnicastRemoteObject
        }
        catch (RemoteException ex) {
            ex.printStackTrace();
        }
    }
}

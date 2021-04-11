package test_cases.wordcount;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;

public class WordCount {

    public static void main(String[] args)
    {
        MapReduceSpecification mp = new MapReduceSpecification();
        mp.numProcesses = 2;
        mp.inputFileLocation =  System.getProperty("user.dir")+"/src/data/hamlet.txt";
        mp.outputFileLocation =  System.getProperty("user.dir")+"/src/test_cases_output/wordcount";
        File directory = new File(mp.outputFileLocation);
        directory.mkdir();
        mp.mapperClassPath = "test_cases.wordcount.WordCountMapper";
        mp.reducerClassPath = "test_cases.wordcount.WordCountReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mp);
    }
}

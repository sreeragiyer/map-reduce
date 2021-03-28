package test_cases.wordcount;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;
import mapreduce.utils.Mapper;
import test_cases.wordlength.WordLengthMapper;
import test_cases.wordlength.WordLengthReducer;

public class WordCount {

    public static void main(String[] args)
    {
        MapReduceSpecification mp = new MapReduceSpecification();
        mp.numProcesses = 1;
        mp.inputFileLocation =  System.getProperty("user.dir")+"/src/data/hamlet.txt";
        mp.outputFileLocation =  System.getProperty("user.dir")+"/src/test_cases_output/wordcount.txt";
        mp.mapper = new WordCountMapper();
        mp.reducer = new WordCountReducer();
        MapReduce obj = new MapReduce();
        obj.mapReduce(mp);



    }







}

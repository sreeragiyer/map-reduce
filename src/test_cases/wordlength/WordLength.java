package test_cases.wordlength;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.IOException;

public class WordLength {

    public  static void main(String[] args) throws IOException {
        System.out.println("Hello world");
        MapReduceSpecification mrs = new MapReduceSpecification();
        mrs.numProcesses = 1;
        mrs.inputFileLocation = "/Users/mahidhar/Documents/532/p1_mapreduce-team-18/out/production/MapReduce/data/loremipsum.txt";
        mrs.outputFileLocation = "/Users/mahidhar/Documents/532/p1_mapreduce-team-18/src/test_cases_output/WordLength.txt";
        mrs.mapper = new WordLengthMapper();
        mrs.reducer = new WordLengthReducer();
        MapReduce obj = new MapReduce();
        obj.mapReduce(mrs);
    }
}

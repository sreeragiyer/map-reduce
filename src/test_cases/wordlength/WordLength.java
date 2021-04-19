package test_cases.wordlength;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;

public class WordLength {

    public  static void main(String[] args) {
        MapReduceSpecification mrs = new MapReduceSpecification();
        mrs.numProcesses = 3;
        mrs.inputFileLocation = System.getProperty("user.dir")+"/src/data/loremipsum.txt";
        mrs.outputFileLocation = System.getProperty("user.dir")+"/src/test_cases_output/wordlength";
        File directory = new File(mrs.outputFileLocation);
        directory.mkdir();
        mrs.mapperClassPath = "test_cases.wordlength.WordLengthMapper";
        mrs.reducerClassPath = "test_cases.wordlength.WordLengthReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mrs);
    }
}

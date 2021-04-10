package test_cases.wordlength;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

public class WordLength {

    public  static void main(String[] args) {
        MapReduceSpecification mrs = new MapReduceSpecification();
        mrs.numProcesses = 1;
        mrs.inputFileLocation = System.getProperty("user.dir")+"/src/data/loremipsum.txt";
        mrs.outputFileLocation = System.getProperty("user.dir")+"/src/test_cases_output/WordLength.txt";
        mrs.mapper = new WordLengthMapper();
        mrs.reducer = new WordLengthReducer();
        mrs.reducerClassPath = "test_cases.wordlength.WordLengthReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mrs);
    }
}

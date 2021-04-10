package test_cases.capitalize;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

public class Capitalize {

    public static void main(String[] args) {

        MapReduceSpecification mrs = new MapReduceSpecification();
        mrs.numProcesses = 1;
        mrs.inputFileLocation =  System.getProperty("user.dir")+"/src/data/loremipsum.txt";
        mrs.outputFileLocation =  System.getProperty("user.dir")+"/src/test_cases_output/Capitalize.txt";
        mrs.mapper = new CapitalizeMapper();
        mrs.reducer = new CapitalizeReducer();
        MapReduce obj = new MapReduce();
        obj.mapReduce(mrs);
    }
}
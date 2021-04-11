package test_cases.capitalize;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;

public class Capitalize {

    public static void main(String[] args) {
        MapReduceSpecification mrs = new MapReduceSpecification();
        mrs.numProcesses = 2;
        mrs.inputFileLocation =  System.getProperty("user.dir")+"/src/data/loremipsum.txt";
        mrs.outputFileLocation =  System.getProperty("user.dir")+"/src/test_cases_output/capitalize";
        File directory = new File(mrs.outputFileLocation);
        directory.mkdir();
        mrs.mapperClassPath = "test_cases.capitalize.CapitalizeMapper";
        mrs.reducerClassPath = "test_cases.capitalize.CapitalizeReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mrs);
    }
}
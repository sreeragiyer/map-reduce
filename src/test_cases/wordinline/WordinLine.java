package test_cases.wordinline;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;

public class WordinLine {

    public static void main(String[] args)
    {
        MapReduceSpecification mp = new MapReduceSpecification();
        mp.numProcesses = 2;
        mp.inputFileLocation = System.getProperty("user.dir")+"/src/data/hamlet.txt";
        mp.outputFileLocation = System.getProperty("user.dir")+"/src/test_cases_output/wordinline";
        File directory = new File(mp.outputFileLocation);
        directory.mkdir();
        mp.mapperClassPath = "test_cases.wordinline.WordinLineMapper";
        mp.reducerClassPath = "test_cases.wordinline.WordinLineReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mp);
    }

}

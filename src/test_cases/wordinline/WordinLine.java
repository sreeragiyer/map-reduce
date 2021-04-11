package test_cases.wordinline;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;
import mapreduce.utils.Mapper;
import test_cases.wordinline.WordinLineMapper;
import test_cases.wordinline.WordinLineReducer;

public class WordinLine {

    public static void main(String[] args)
    {
        MapReduceSpecification mp = new MapReduceSpecification();
        mp.numProcesses = 1;
        mp.inputFileLocation = System.getProperty("user.dir")+"/src/data/hamlet.txt";
        mp.outputFileLocation = System.getProperty("user.dir")+"/src/test_cases_output/wordinline.txt";
        mp.mapper = new WordinLineMapper();
        mp.reducerClassPath = "test_cases.wordinline.WordinLineReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mp);
    }

}

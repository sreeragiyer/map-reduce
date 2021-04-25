package test_cases.wordinline;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.File;
import java.rmi.RemoteException;

public class WordinLine {

    public static void main(String[] args)
    {
        try {
            MapReduceSpecification mp = new MapReduceSpecification();
            mp.numProcesses = 2;
            mp.inputFileLocation = System.getProperty("user.dir") + "/data/hamlet.txt"; // test cases will be run from the root dir
            mp.outputFileLocation = System.getProperty("user.dir") + "/test_cases_output/wordinline";
            File directory = new File(mp.outputFileLocation);
            directory.mkdir();
            mp.mapperKey = "WordinLineMapper";
            mp.reducerKey = "WordinLineReducer";
            mp.mapper = new WordinLineMapper();
            mp.reducer = new WordinLineReducer();
            MapReduce obj = new MapReduce();
            obj.mapReduce(mp);
            System.exit(0);
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }

}

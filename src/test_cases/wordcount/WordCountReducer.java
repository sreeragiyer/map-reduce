package test_cases.wordcount;

import mapreduce.utils.Reducer;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

/**
 * User defined reducer class
 */
public class WordCountReducer extends UnicastRemoteObject implements Reducer, Serializable {
    // has to extend UnicastRemoteObject to get added to RMI registry

    public WordCountReducer() throws RemoteException {
        super();
    }

    // user defined reduce method
    public List<String> reduce(String strin, List<String> list) throws RemoteException
    {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        // add all the counts in list to get the total count
        for(String str : list)
        {
            try {
                count += Integer.parseInt(str);
            }
            catch (Exception e){}
        }

        String res = Integer.toString(count);
        result.add(res);

        return result;

    }
}

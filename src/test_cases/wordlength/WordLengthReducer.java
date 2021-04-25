package test_cases.wordlength;


import mapreduce.utils.Reducer;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * User defined mapper class
 */
public class WordLengthReducer extends UnicastRemoteObject implements Reducer, Serializable {
    // has to extend UnicastRemoteObject to get added to RMI registry

    public WordLengthReducer() throws RemoteException {
        super();
    }

    // user defined reduce method
    public List<String> reduce(String len, List<String> words) throws RemoteException {
        // remove duplicates and join the list
        String res = String.join(",", new ArrayList<String>(new HashSet<>(words)));
        List<String> joined = new ArrayList<>();
        joined.add(res);
        return joined;
    }
}

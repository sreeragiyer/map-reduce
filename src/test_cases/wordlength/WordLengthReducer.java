package test_cases.wordlength;


import mapreduce.utils.Reducer;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class WordLengthReducer extends UnicastRemoteObject implements Reducer, Serializable {
    public WordLengthReducer() throws RemoteException {
        super();
    }
    public List<String> reduce(String len, List<String> words) throws RemoteException {
        String res = String.join(",", words);
        List<String> joined = new ArrayList<>();
        joined.add(res);
        return joined;
    }
}

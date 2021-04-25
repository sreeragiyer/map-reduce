package mapreduce.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * The Reducer interface that user has to implement
 */
public interface Reducer extends Remote {
    // reduce function that the user will define
    List<String> reduce(String key, List<String> values) throws RemoteException;
}

package mapreduce.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

/**
 * The Mapper interface that user has to implement
 */
public interface Mapper extends Remote {
    // map function that the user will define
    HashMap<String, List<String>> map(String k, String v) throws RemoteException;
}

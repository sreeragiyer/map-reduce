package mapreduce.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;


public interface Mapper extends Remote {
    HashMap<String, List<String>> map(String k, String v) throws RemoteException;
}

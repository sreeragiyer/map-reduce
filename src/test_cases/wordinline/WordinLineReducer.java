package test_cases.wordinline;
import mapreduce.utils.Reducer;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class WordinLineReducer extends UnicastRemoteObject implements Reducer, Serializable {

    public WordinLineReducer() throws RemoteException {
        super();
    }

    public List<String> reduce(String word,List<String> lines) throws RemoteException {
         List<String> result = new ArrayList<String>();
         lines = new ArrayList<>(new HashSet<>(lines)); // removing duplicate line numbers
         StringBuilder sb = new StringBuilder();
         for(String line : lines)
         {
             sb.append(line);
             sb.append(",");
         }

         if(sb.length() > 0)
         {
             sb.deleteCharAt(sb.length()-1);
         }

         String res = sb.toString();

         result.add(res);

         return result;


    }
}

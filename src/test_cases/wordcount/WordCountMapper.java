package test_cases.wordcount;
import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import mapreduce.utils.Mapper;

public class WordCountMapper extends UnicastRemoteObject implements Mapper, Serializable {

    public WordCountMapper() throws RemoteException {
        super();
    }

    public HashMap<String,List<String>> map(String docId,String input) throws RemoteException
    {
        HashMap<String,List<String>> map = new HashMap<>();
        String[] words = input.toLowerCase(Locale.ROOT).split("\\s+");

        for(String str : words)
        {
            if(str.length()>0) {
                String updatedString = str.replaceAll("[^a-zA-Z0-9\\']", "");
                map.computeIfAbsent(updatedString, k -> new ArrayList<String>());
                map.get(updatedString).add("1");
            }
        }

        return map;

    }
}

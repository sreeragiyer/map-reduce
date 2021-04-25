package test_cases.wordcount;
import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import mapreduce.utils.Mapper;

/**
 * User defined mapper class
 */
public class WordCountMapper extends UnicastRemoteObject implements Mapper, Serializable {
    // has to extend UnicastRemoteObject to get added to RMI registry

    public WordCountMapper() throws RemoteException {
        super();
    }

    // takes a word and maps word -> 1
    public HashMap<String,List<String>> map(String docId,String input) throws RemoteException
    {
        HashMap<String,List<String>> map = new HashMap<>();
        // input will be a part of the text file, splitting it to words
        String[] words = input.toLowerCase(Locale.ROOT).split("\\s+");

        // for each word, map to 1
        for(String str : words)
        {
            if(str.length()>0) {
                String updatedString = str.replaceAll("[^a-zA-Z0-9\\']", "");
                map.computeIfAbsent(updatedString, k -> new ArrayList<String>());
                map.get(updatedString).add("1"); // word -> 1
            }
        }
        return map;

    }
}

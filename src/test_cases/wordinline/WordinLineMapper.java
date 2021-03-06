package test_cases.wordinline;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import mapreduce.utils.Mapper;

/**
 * User defined mapper class
 */
public class WordinLineMapper extends UnicastRemoteObject implements Mapper, Serializable {
    // has to extend UnicastRemoteObject to get added to RMI registry

    public WordinLineMapper() throws RemoteException {
        super();
    }

    // maps word-> line number
    public HashMap<String,List<String>> map(String start_line,String input) throws RemoteException
    {

        int sl = Integer.parseInt(start_line);
        HashMap<String,List<String>> map = new HashMap<>();
        // The below regular expression splits the string by the different lines
        String[] lines = input.split("\\r?\\n");
        List<String> temp = new ArrayList<String>();
        for(String liner : lines)
        {
            // This removes all the empty lines before adding to the temp Arraylist
            temp.add(liner);

        }

        int count = temp.size();
        String[] flines = new String[count];
        int index = 0;
        for(String stri : temp)
        {
            flines[index++] = stri;
        }

        int line = 1;
        for(String str : flines)
        {
            String[] words = str.toLowerCase(Locale.ROOT).split("\\s+");
            for(String strin : words)
            {
                //This makes sure that only proper meaningful words are added to our map
                if (strin != null)
                {
                    String updatedString = strin.replaceAll("[^a-zA-Z0-9\\']", "").toLowerCase();
                    map.computeIfAbsent(updatedString,k -> new ArrayList<String>());
                    map.get(updatedString).add(Integer.toString(sl+line));
                }

            }

            line++;
        }

        return map;


    }


}

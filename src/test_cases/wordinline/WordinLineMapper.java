package test_cases.wordinline;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import mapreduce.utils.Mapper;

public class WordinLineMapper extends UnicastRemoteObject implements Mapper, Serializable {

    public WordinLineMapper() throws RemoteException {
        super();
    }

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
                if(strin != null && strin.matches("^[a-zA-Z]*$"))
                {
                map.computeIfAbsent(strin,k -> new ArrayList<String>());
                map.get(strin).add(Integer.toString(sl+line));
                }
            }

            line++;
        }

        return map;


    }


}

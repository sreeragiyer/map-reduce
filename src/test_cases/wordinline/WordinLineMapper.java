package test_cases.wordinline;
import java.io.*;
import java.util.*;
import mapreduce.utils.Mapper;

public class WordinLineMapper extends Mapper {

    public HashMap<String,List<String>> map(String docId,String input)
    {
        HashMap<String,List<String>> map = new HashMap<>();
        // The below regular expression splits the string by the different lines
        String[] lines = input.split("\\r?\\n");
        List<String> temp = new ArrayList<String>();
        for(String liner : lines)
        {
            // This removes all the empty lines before adding to the temp Arraylist
            if(!liner.trim().isEmpty())
            {
                temp.add(liner);
            }
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
                map.get(strin).add(Integer.toString(line));
                }
            }

            line++;
        }

        return map;


    }


}

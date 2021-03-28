package test_cases.wordcount;
import java.io.*;
import java.util.*;
import mapreduce.utils.Mapper;

public class WordCountMapper extends Mapper {

    public HashMap<String,List<String>> map(String docId,String input)
    {
        HashMap<String,List<String>> map = new HashMap<>();
        String[] words = input.toLowerCase(Locale.ROOT).split("\\s+");

        System.out.println(words.length);
        for(String str : words)
        {
            if(str.length()>0) {
                map.computeIfAbsent(str, k -> new ArrayList<String>());
                map.get(str).add("1");
            }
        }

        return map;

    }
}

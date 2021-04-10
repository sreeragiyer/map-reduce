package test_cases.wordcount;

import mapreduce.utils.Reducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WordCountReducer extends Reducer {

    public List<String> reduce(String strin, List<String> list)
    {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        for(String str : list)
        {
            try {
                count += Integer.parseInt(str);
            }
            catch (Exception e){}
        }

        String res = Integer.toString(count);
        result.add(res);

        return result;

    }
}

package test_cases.WordLengthTestCase;


import mapreduce.utils.Reducer;

import java.util.ArrayList;
import java.util.List;

public class WordLengthReducer extends Reducer {
    public List<String> reduce(String len, List<String> words) {
        String res = String.join(",", words);
        List<String> joined = new ArrayList<>();
        joined.add(res);
        return joined;
    }
}

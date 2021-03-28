package test_cases.capitalize;

import java.util.ArrayList;
import java.util.List;
import mapreduce.utils.Reducer;

public class CapitalizeReducer extends Reducer{
    public List<String> reduce(String k, List<String> words) {
        return words;
    }
}

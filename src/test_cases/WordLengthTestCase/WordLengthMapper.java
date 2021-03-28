package test_cases.WordLengthTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapreduce.utils.Mapper;

public class WordLengthMapper extends Mapper {
    public HashMap<String, List<String>> map(String docId, String txt) {
        HashMap<String, List<String> > same_len_words = new HashMap<String, List<String>>();
        int txt_len = txt.length();
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<txt_len;i++) {
            if (Character.isWhitespace(txt.charAt(i)) || txt.charAt(i) == '.' || txt.charAt(i) == '\'') {
                String word = sb.toString();
                String len = String.valueOf(word.length());
                List<String> words = same_len_words.get(len);
                if(words == null || words.isEmpty()) {
                    words = new ArrayList<>();
                }
                words.add(word);
                same_len_words.put(len,words);
                sb = new StringBuilder();
            }
            else {
                sb.append(txt.charAt(i));
            }
        }
        return same_len_words;
    }
}

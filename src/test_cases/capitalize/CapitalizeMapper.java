package test_cases.capitalize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import mapreduce.utils.Mapper;

public class CapitalizeMapper extends Mapper{
    public HashMap<String, List<String>> map(String docId, String txt) {

        HashMap<String, List<String>> m = new HashMap<>();

        int len = txt.length();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len - 1; i++) {

            if (Character.isWhitespace(txt.charAt(i)) || txt.charAt(i) == '.' || txt.charAt(i) == '\'') {
                String word = sb.toString();
                int l =word.length();
                if(l > 0) {
                    sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
                    List<String> title_case = new ArrayList<>();
                    title_case.add(sb.toString());
                    m.put(word,title_case);
                    sb = new StringBuilder();
                }
            }
            else {
                sb.append(txt.charAt(i));
            }
        }

        return m;
    }

    public void printHelloWorld() {
        System.out.println("Hello world");
    }
}

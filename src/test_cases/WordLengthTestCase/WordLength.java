package test_cases.WordLengthTestCase;

import mapreduce.utils.MapReduce;
import mapreduce.utils.MapReduceSpecification;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class WordLength {

//    public HashMap<Integer, List<String>> map(String docId, String txt) {
//        HashMap<Integer, List<String> > same_len_words = new HashMap<Integer, List<String>>();
//        int txt_len = txt.length();
//        StringBuilder sb = new StringBuilder();
//        for(int i=0;i<txt_len;i++) {
//            if (Character.isWhitespace(txt.charAt(i)) || txt.charAt(i) == '.' || txt.charAt(i) == '\'') {
//                String word = sb.toString();
//                int len = word.length();
//                List<String> words = same_len_words.get(len);
//                if(words == null || words.isEmpty()) {
//                    words = new ArrayList<>();
//                }
//                words.add(word);
//                same_len_words.put(len,words);
//                sb = new StringBuilder();
//            }
//            else {
//                sb.append(txt.charAt(i));
//            }
//        }
//        return same_len_words;
//    }
//
//    public List<String> reduce(int len, List<String> words) {
//        String res = String.join(",", words);
//        List<String> joined = new ArrayList<>();
//        joined.add(res);
//        return joined;
//    }

    public  static void main(String[] args) throws IOException {
//        File in = new File("src/data/loremipsum.txt");
//        Scanner sc = new Scanner(in);
//        StringBuilder sb = new StringBuilder();
//        while (sc.hasNextLine()) {
//            String data = sc.nextLine();
//            sb.append(data);
//        }
//        sc.close();
        MapReduceSpecification mrs = new MapReduceSpecification();
        mrs.numProcesses = 1;
        mrs.inputFileLocation = "src/data/loremipsum.txt";
        mrs.outputFileLocation = "src/test_cases_output/WordLength.txt";
        mrs.mapper = new WordLengthMapper();
        mrs.reducer = new WordLengthReducer();
//        WordLength obj = new WordLength();
//        HashMap<Integer, List<String>> same_len_words = obj.map("1", sb.toString());
//        Iterator it = same_len_words.entrySet().iterator();
//        while(it.hasNext()) {
//            HashMap.Entry<Integer, List<String>> pair = (HashMap.Entry)it.next();
//            it.remove();
//            int len = pair.getKey();
//            List<String> words = obj.reduce(len, pair.getValue());
//
//            StringBuilder stringBuilder=new StringBuilder();
//            FileWriter fstream = new FileWriter("src/test_cases_output/WordLength.txt", true);
//            BufferedWriter info = new BufferedWriter(fstream);
//            if(words!=null && !words.isEmpty()) {
//                stringBuilder.append(len+"\t"+words.get(0)+"\n");
//                info.write(len+"\t"+words.get(0)+"\n");
//            }
//            System.out.println(stringBuilder.toString());
//
//
//            info.close();
//            fstream.close();
        MapReduce obj = new MapReduce();
        obj.mapReduce(mrs);



    }
}

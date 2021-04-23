package test_cases.wordlength;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mapreduce.utils.Mapper;

public class WordLengthMapper extends UnicastRemoteObject implements Mapper, Serializable {
    public WordLengthMapper() throws RemoteException {
        super();
    }
    public HashMap<String, List<String>> map(String docId, String txt) throws RemoteException{
        HashMap<String, List<String> > same_len_words = new HashMap<String, List<String>>();
        int txt_len = txt.length();
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<txt_len;i++) {
            if (Character.isWhitespace(txt.charAt(i)) || txt.charAt(i) == '.' || txt.charAt(i) == '\'') {
                String word = sb.toString();
                int l =word.length();
                if(l > 0) {
                    String len = String.valueOf(l);
                    List<String> words = same_len_words.get(len);
                    if (words == null || words.isEmpty()) {
                        words = new ArrayList<>();
                    }
                    words.add(word);
                    same_len_words.put(len, words);
                    sb = new StringBuilder();
                }
            }
            else {
                sb.append(txt.charAt(i));
            }
        }
        return same_len_words;
    }
}

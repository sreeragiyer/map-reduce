package test_cases.wordlength;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mapreduce.utils.Mapper;

/**
 * User defined mapper class
 */
public class WordLengthMapper extends UnicastRemoteObject implements Mapper, Serializable {
    // has to extend UnicastRemoteObject to get added to RMI registry

    public WordLengthMapper() throws RemoteException {
        super();
    }

    // maps length of word to word
    public HashMap<String, List<String>> map(String docId, String txt) throws RemoteException{
        HashMap<String, List<String> > same_len_words = new HashMap<String, List<String>>();
        int txt_len = txt.length();
        StringBuilder sb = new StringBuilder();
        // txt contains some input file, split it into words
        for(int i=0;i<txt_len;i++) {
            if (Character.isWhitespace(txt.charAt(i)) || txt.charAt(i) == '.' || txt.charAt(i) == '\'') {
                String word = sb.toString();
                int l =word.length(); // get length of word
                if(l > 0) {
                    String len = String.valueOf(l);
                    List<String> words = same_len_words.get(len); // get all words of same length
                    if (words == null || words.isEmpty()) {
                        words = new ArrayList<>();
                    }
                    words.add(word);
                    same_len_words.put(len, words); // add to list of all words of same length with length as key
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

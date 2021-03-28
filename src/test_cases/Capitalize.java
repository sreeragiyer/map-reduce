package test_cases;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import mapreduce.utils.Mapper;

public class Capitalize extends  Mapper {

    public HashMap<String, List<String>> map(String s, String v) {

        HashMap<String, List<String>> m = new HashMap<>();

        return m;
    }

    public static void main(String[] args) throws IOException {
        File in = new File("src/data/loremipsum.txt");
        Scanner sc = new Scanner(in);
        StringBuilder sb = new StringBuilder();

        while (sc.hasNextLine()) {
            String data = sc.nextLine();
            sb.append(data);
        }
        sc.close();

        int len = sb.length();
        for (int i = 0; i < len - 1; i++) {
            if (Character.isWhitespace(sb.charAt(i)) || sb.charAt(i) == '.' || sb.charAt(i) == '\'') {
                sb.setCharAt(i + 1, Character.toUpperCase(sb.charAt(i + 1)));
            }
        }

        FileWriter myWriter = new FileWriter("src/test_cases_output/capitalized.txt");
        myWriter.write(sb.toString());
        myWriter.close();


    }
}
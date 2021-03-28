package test_cases;

import java.io.*;
import java.util.Scanner;

public class Capitalize {
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

        FileWriter myWriter = new FileWriter("src/data/capitalized.txt");
        myWriter.write(sb.toString());
        myWriter.close();


    }
}
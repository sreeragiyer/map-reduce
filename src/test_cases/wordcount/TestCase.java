package test_cases.wordcount;

public class TestCase {

    public static void main(String[] args) {
        String s = "Sister;";
        System.out.println("Updated String : " + s.replaceAll("[^a-zA-Z]", "").toLowerCase());
    }
}
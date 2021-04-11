package mapreduce.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public abstract class Mapper {

    protected abstract HashMap<String, List<String>> map(String k, String v);

    public String execute(String docId, String inputText) {
        HashMap<String, List<String>> output = map(docId, inputText);
        String currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File directory = new File("reducer_0");
        directory.mkdir();
        String tempFileName = "reducer_0/intermediate_" + currentTime + ".txt";

        File tempFile = new File(tempFileName);
        try {
            tempFile.createNewFile();
            FileWriter fw = new FileWriter(tempFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            output.entrySet().forEach(entry -> {
                entry.getValue().forEach(value -> {
                    try {
                        bw.write(entry.getKey() + ":" + value);
                        bw.write("\n");
                    } catch (IOException e) {
                        //TODO : Handle the exception with proper messaging.
                    }
                });
            });
            bw.close();
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }
        return tempFileName;
    }

    public void execute(String inputFileLoc, int start_line, int end_line, int num_processes) {
        StringBuilder inputText = new StringBuilder();
        try {
            FileInputStream fs = new FileInputStream(inputFileLoc);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs));
            for (int i = 0; i < start_line; ++i)
                br.readLine();
            for (int i = start_line; i<=end_line; i++) {
                inputText.append(br.readLine());
                inputText.append("\n");
            }
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }

        String docId = "";
        HashMap<String, List<String>> output = map(docId, inputText.toString());
        String currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String tempFileName = "intermediate_" + currentTime + ".txt";
        List<BufferedWriter> bufferedWriters = new ArrayList<>();
        try {
            for (int i = 0; i < num_processes; i++) {
                File directory = new File("reducer_" + i);
                directory.mkdir();
                File tempFile = new File("reducer_" + i + "/" + tempFileName);
                if (!tempFile.exists()) {
                    tempFile.createNewFile();
                }
                FileWriter fw = new FileWriter(tempFile.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bufferedWriters.add(bw);
            }
            output.forEach((key, value) -> {
                int hash_value = (key.hashCode())%num_processes;
                value.forEach(val -> {
                    try {
                        BufferedWriter bufferedWriter = bufferedWriters.get(hash_value);
                        bufferedWriter.write(key + ":" + val);
                        bufferedWriter.write("\n");
                    } catch (IOException e) {
                        //TODO : Handle the exception with proper messaging.
                    }
                });
            });
            for (BufferedWriter bufferedWriter : bufferedWriters) {
                bufferedWriter.close();
            }

        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }
    }

    public static void main(String[] args) {
        try {
            String className = args[0];
            String inputFileLoc = args[1];
            String start_line = args[2];
            String end_line = args[3];
            String num_processes = args[4];
            Mapper obj = (Mapper) Class.forName(className).getDeclaredConstructor().newInstance();
            String data = new String(Files.readAllBytes(Paths.get(inputFileLoc)));
           // obj.execute("", data);
            obj.execute(inputFileLoc, Integer.parseInt(start_line), Integer.parseInt(end_line),
                    Integer.parseInt(num_processes));
        }
        catch(Exception e) {
            System.out.println("class not found");
        }
    }
}

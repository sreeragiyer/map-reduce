package mapreduce.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public interface Mapper extends Remote {

    HashMap<String, List<String>> map(String k, String v) throws RemoteException;

//    public void execute(String inputFileLoc, int start_line, int end_line, int num_processes, int procees_num) {
//
//        StringBuilder inputText = new StringBuilder();
//        try {
//            FileInputStream fs = new FileInputStream(inputFileLoc);
//            BufferedReader br = new BufferedReader(new InputStreamReader(fs));
//            for (int i = 0; i < start_line; ++i)
//                br.readLine();
//            for (int i = start_line; i<=end_line; i++) {
//                inputText.append(br.readLine());
//                inputText.append("\n");
//            }
//            br.close();
//        } catch (IOException e) {
//            //TODO : Handle the exception with proper messaging.
//        }
//
//        String docId = "";
//        HashMap<String, List<String>> output = map(docId, inputText.toString());
//        List<BufferedWriter> bufferedWriters = new ArrayList<>();
//        try {
//            String currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
//            String tempFileName = "intermediate_" + currentTime + "_" + procees_num + ".txt";
//            for (int i = 0; i < num_processes; i++) {
//                File directory = new File("reducer_" + i);
//                if (!directory.exists()) directory.mkdir();
//
//                File tempFile = new File("reducer_" + i + "/" + tempFileName);
//                if (!tempFile.exists()) {
//                    tempFile.createNewFile();
//                }
//                FileWriter fw = new FileWriter(tempFile.getAbsoluteFile());
//                BufferedWriter bw = new BufferedWriter(fw);
//                bufferedWriters.add(bw);
//            }
//            output.forEach((key, value) -> {
//                int hash_value = Math.abs(key.hashCode())%num_processes;
//                value.forEach(val -> {
//                    try {
//                        BufferedWriter bufferedWriter = bufferedWriters.get(hash_value);
//                        bufferedWriter.write(key + ":" + val);
//                        bufferedWriter.write("\n");
//                    } catch (IOException e) {
//                        //TODO : Handle the exception with proper messaging.
//                    }
//                });
//            });
//            for (BufferedWriter bufferedWriter : bufferedWriters) {
//                bufferedWriter.close();
//            }
//
//        } catch (IOException e) {
//            //TODO : Handle the exception with proper messaging.
//        }
//    }
//
//    public static void main(String[] args) {
//        try {
//            String className = args[0];
//            String inputFileLoc = args[1];
//            String start_line = args[2];
//            String end_line = args[3];
//            String num_processes = args[4];
//            String process_num = args[5];
//            Mapper obj = (Mapper) Class.forName(className).getDeclaredConstructor().newInstance();
//            obj.execute(inputFileLoc, Integer.parseInt(start_line), Integer.parseInt(end_line),
//                    Integer.parseInt(num_processes), Integer.parseInt(process_num));
//        }
//        catch (Exception e) {
//            System.out.println("class not found");
//        }
//    }
}

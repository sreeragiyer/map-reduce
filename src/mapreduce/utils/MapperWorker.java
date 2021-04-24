package mapreduce.utils;

import java.io.*;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class MapperWorker {
    public void execute(Mapper obj, String inputFileLoc, int start_line, int end_line, int num_processes, int procees_num) {


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
            br.close();
        } catch (IOException e) {
            //TODO : Handle the exception with proper messaging.
        }


        try {
            HashMap<String, List<String>> output = obj.map(String.valueOf(start_line), inputText.toString());
            List<BufferedWriter> bufferedWriters = new ArrayList<>();
            try {
                String currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                String tempFileName = "intermediate_" + currentTime + "_" + procees_num + ".txt";
                for (int i = 0; i < num_processes; i++) {
                    File directory = new File("reducer_" + i);
                    if (!directory.exists()) directory.mkdir();

                    File tempFile = new File("reducer_" + i + "/" + tempFileName);
                    if (!tempFile.exists()) {
                        tempFile.createNewFile();
                    }
                    FileWriter fw = new FileWriter(tempFile.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
                    bufferedWriters.add(bw);

                }
                output.forEach((key, value) -> {
                    int hash_value = Math.abs(key.hashCode()) % num_processes;
                    value.forEach(val -> {
                        try {
                            BufferedWriter bufferedWriter = bufferedWriters.get(hash_value);
                            bufferedWriter.write(key + ":" + val+"~@"+start_line+"~$"+end_line);
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
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
//            String className = args[0];
//            String inputFileLoc = args[1];
//            String start_line = args[2];
//            String end_line = args[3];
//            String num_processes = args[4];
//            String process_num = args[5];
            String mapperKey = args[0];
            String inputFileLoc = args[1];
            String start_line = args[2];
            String end_line = args[3];
            String num_processes = args[4];
            String process_num = args[5];
            //Mapper obj = (Mapper) Class.forName(className).getDeclaredConstructor().newInstance();
            MapReduce mr = new MapReduce();
            Mapper obj = mr.getMapperObj(mapperKey);
            MapperWorker w = new MapperWorker();
            w.execute(obj, inputFileLoc, Integer.parseInt(start_line), Integer.parseInt(end_line),
                    Integer.parseInt(num_processes), Integer.parseInt(process_num));
        }
        catch (Exception e) {
            System.out.println("class not found");
        }
    }
}

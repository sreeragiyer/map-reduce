package mapreduce.utils;

import java.io.*;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * The MapperWorker class that defines the working of a worker node acting as a mapper
 */
public class MapperWorker {

    // method to read from input file and write to intermediate file
    public void execute(Mapper obj, String inputFileLoc, int start_line, int end_line, int num_processes, int procees_num) {
        StringBuilder inputText = new StringBuilder();
        // read from input file
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
            e.printStackTrace();
        }

        // after reading input data, create writers to N intermediate files (for each mapper)
        try {
            HashMap<String, List<String>> output = obj.map(String.valueOf(start_line), inputText.toString());
            List<BufferedWriter> bufferedWriters = new ArrayList<>();
            try {
                // intermediate file name will be of the form intermediate_dateTime_processNUmber.txt
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
                    // hash key (String) to an integer between 0 and N
                    // This ensures that all keys go to the same file
                    // No shuffling of keys required later.
                    int hash_value = Math.abs(key.hashCode()) % num_processes;
                    value.forEach(val -> {
                        try {
                            BufferedWriter bufferedWriter = bufferedWriters.get(hash_value);
                            bufferedWriter.write(key + ":" + val);
                            bufferedWriter.write("\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                });
                for (BufferedWriter bufferedWriter : bufferedWriters) {
                    bufferedWriter.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        catch(RemoteException ex) {
            ex.printStackTrace();
        }
    }

    // will be invoked by the master as a separate process
    public static void main(String[] args) throws InterruptedException {
        // read all input parameters provided by the master
        String mapperKey = args[0];
        String inputFileLoc = args[1];
        String start_line = args[2];
        String end_line = args[3];
        String num_processes = args[4];
        String process_num = args[5];
        int timeout = Integer.parseInt(args[6]);
        timeout = timeout > 0 ? timeout : 6000;

        // starting as a new thread to ensure time limit
        Runnable task = () -> {
            while(args.length > 7 && Integer.parseInt(process_num) == Integer.parseInt(args[7])); // to simulate fault tolerance
            MapReduce mr = new MapReduce();
            Mapper obj = null;
            try {
                obj = mr.getMapperObj(mapperKey); // get user defined mapper object from RMI registry
            }
            catch (RemoteException | NotBoundException | MalformedURLException e) {
                e.printStackTrace();
                System.exit(2);
            }
            MapperWorker w = new MapperWorker();
            w.execute(obj, inputFileLoc, Integer.parseInt(start_line), Integer.parseInt(end_line),
                    Integer.parseInt(num_processes), Integer.parseInt(process_num));

        };
        Thread mapThread = new Thread(task);
        mapThread.start();
        mapThread.join(timeout);
        // if time limit exceeded, then stop this process
        if(mapThread.isAlive()) {
            System.exit(1);
        }

    }
}

package store_test;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ConcurrentFileReader {
    private static final int THREAD_COUNT = 4; // 线程数量

    public static void testParallelism(String filePath) {

        long fileSize = new File(filePath).length();
        long partSize = fileSize / THREAD_COUNT;

        List<FileReadThread> threads = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            long start = i * partSize;
            long end = (i + 1) == THREAD_COUNT ? fileSize : (i + 1) * partSize;
            System.out.println("start: " +start + " end: " + end + " sub: " + (end - start));
            FileReadThread thread = new FileReadThread(filePath, start, end);
            threads.add(thread);
            thread.start();
        }

        for(FileReadThread thread : threads){
            try{
                thread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        int sum = 0;
        for(FileReadThread task : threads){
            sum += task.getCount();
        }
        System.out.println("sum: " + sum);

    }

    public static void testSingleThread(String filePath){
        int sum = 0;
        long fileSize = new File(filePath).length();

        System.out.println("fileSize: " + fileSize);

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(0);
            byte[] buffer = new byte[8 * 1024]; // 缓冲区
            long bytesRead = 0;

            while (bytesRead < fileSize) {
                int read = raf.read(buffer);
                if (read == -1) break;
                bytesRead += read;
                // 处理读取的数据
                for(int i = 0; i < read; i++){
                    if(buffer[i] == 'R'){
                        sum++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("sum: " + sum);
    }

    public static void main(String[] args){
        String sep = File.separator;
        //String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        //String filePath = prefixPath + "dataset" + sep + "crimes.csv";
        String prefixPath = System.getProperty("user.dir") + sep + "event_store" + sep;
        String filePath = prefixPath + "CRIMES.store";
        System.out.println("filePath: " + filePath);

        long startTime = System.currentTimeMillis();
        // sum: 6307520
        //testParallelism(filePath);
        // sum: 6909989, cost: 472ms-500
        testSingleThread(filePath);
        long endTime = System.currentTimeMillis();
        System.out.println("cost: " + (endTime - startTime) + "ms");
    }
}
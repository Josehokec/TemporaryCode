package store_test;


import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

class FileReadThread extends Thread {
    private final String filePath;
    private final long start;
    private final long end;
    private AtomicInteger count;

    public FileReadThread(String filePath, long start, long end) {
        this.filePath = filePath;
        this.start = start;
        this.end = end;
        count = new AtomicInteger(0);
        //count = 0;
    }

    public int getCount(){
        return count.get();
    }

    @Override
    public void run() {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(start);
            byte[] buffer = new byte[4096];
            long bytesRead = 0;
            while (bytesRead < (end - start)) {
                int read = raf.read(buffer);
                if (read == -1) break;

                if(bytesRead + read >= (end - start)){
                    read = (int) (end - start - bytesRead);
                }

                bytesRead += read;
                // 处理读取的数据
                for(int i = 0; i < read; i++){
                    if(buffer[i] == 'R'){
                        count.incrementAndGet();
                        //count++;
                    }
                }
            }
            System.out.println("bytesRead: " + bytesRead);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

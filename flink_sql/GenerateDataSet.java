package flink_sql;

import java.io.File;
import java.io.PrintStream;
import java.util.Random;

/**
 * synthetic dataset generator
 * skew = 0.5
 */
public class GenerateDataSet {

    /**
     * generate zipf distribution
     * @param   alpha   skew param
     * @return          probability
     */
    public static double[] zipfRangeInterval(int eventTypeNum, double alpha){
        double[] ans = new double[eventTypeNum];
        double C = 0;
        for(int i = 1; i <= eventTypeNum; ++i){
            C += Math.pow((1.0 / i), alpha);
        }
        for(int i = 1; i <= eventTypeNum; ++i){
            double pro = 1.0 / (Math.pow(i, alpha) * C);
            ans[i - 1] = pro;
        }

        return ans;
    }

    public static void generateDataset(){
        String dir = System.getProperty("user.dir");
        String sep = File.separator;
        // 10_000_000
        int eventNum = 1_000_000;
        String filePath = dir + sep + "src" + sep + "main" + sep + "dataset" + sep + "event_" + eventNum + ".csv";
        try{
            System.setOut(new PrintStream(filePath));
        }catch (Exception e){
            e.printStackTrace();
        }

        // we only generate

        int eventTypeNum = 15;
        long beginTime = 1_000_000;
        Random typeRandom = new Random(1);
        Random value1Random = new Random(2);
        Random value2Random = new Random(3);
        Random infoRandom = new Random(4);
        Random timeRandom = new Random(5);

        double[] probability = zipfRangeInterval(eventTypeNum, 0.5);
        // Cumulative Distribution Function
        double[] cdf = new double[eventTypeNum];
        cdf[eventTypeNum - 1] = 1;
        cdf[0] = probability[0];

        for(int i = 1; i < eventTypeNum - 1; ++i){
            cdf[i] = cdf[i - 1] + probability[i];
            // System.out.println("cdf[" + i + "]: " + cdf[i]);
        }

        String csVHeader = "EventType(String),ID(int),Value1(float),Value2(long),Timestamp(long)";
        System.out.println(csVHeader);

        for(int id = 1; id <= eventNum; id++){
            // 15 event types ~ Zipf(skew = 0.6), id++, value1 ~ U[0.00,100.00), value2 ~ U[0, 100), info, delta_timestamp ~ U[1,9];
            StringBuilder eventStr = new StringBuilder(256);
            double pro = typeRandom.nextDouble();

            int[] cnt = new int[eventTypeNum];
            // here can use binary search to optimize
            int left = 0;
            int right = eventTypeNum - 1;
            int mid = (left + right) >> 1;
            while(left <= right){
                if(pro <= cdf[mid]){
                    if(mid == 0 || pro > cdf[mid - 1]){
                        break;
                    }else{
                        right = mid - 1;
                        mid = (left + right) >> 1;
                    }
                }else{
                    if(mid == eventTypeNum - 1){
                        break;
                    }else{
                        left = mid + 1;
                        mid = (left + right) >> 1;
                    }
                }
            }

            int typeId = mid;
            cnt[mid]++;
            char curType = (char) ('A' + mid);
            // 15 event types ~ Zipf(skew = 0.6), id++
            eventStr.append(curType).append(",").append(id).append(",");

            // value1 ~ U[0.00,100.00), value2 ~ U[0, 100), info, delta_timestamp ~ U[1,9];
            double value1 = value1Random.nextDouble() * 100;
            String value1Str = String.format("%.2f", value1);
            long value2 = value2Random.nextInt(100);
            String info = "info_" + infoRandom.nextInt(30);
            beginTime += timeRandom.nextInt(9) + 1;
            eventStr.append(value1Str).append(",").append(value2).append(",").append(info).append(",").append(beginTime);

            System.out.println(eventStr);
        }
    }

    public static void main(String[] args) {
        //zipfRangeInterval(20, 0.7);
        generateDataset();
    }
}

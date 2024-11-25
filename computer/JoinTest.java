package computer;

import utils.ReplayIntervals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/*
这个程序是验证我们window bloom filter方法过滤 正确性的
 */


public class JoinTest {
    static class Pair<K,V> {
        private final K key;
        private final V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    public static void main(String[] args) {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        // crimes_q1_ultra: 1878     join cnt: 1878, r: 1385, b: 1786, m: 3521, sum: 6692
        // crimes_q1_12_10_10_old: 1913
        // crimes_q1_interval        join cnt: 1766, r: 1356, b: 1752, m: 3450, sum: 6558
        String outputFilePath = prefixPath + "output" + sep + "crimes_q1_interval.txt";
        long window = 1800;
        ReplayIntervals r = new ReplayIntervals();
        ReplayIntervals b = new ReplayIntervals();
        ReplayIntervals m = new ReplayIntervals();

        List<Pair<Long, String>> rPairs = new ArrayList<>(1024);
        List<Pair<Long, String>> bPairs = new ArrayList<>(1024);
        List<Pair<Long, String>> mPairs = new ArrayList<>(1024);

        Map<Long, Set<String>> mJoinMap = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(outputFilePath))) {
            // primary_type,id,beat,district,latitude,longitude,timestamp
            String line;
//            line = br.readLine();
            while ((line = br.readLine()) != null) {
                String[] splits = line.split(",");
                long ts = Long.parseLong(splits[6]);

                String type = splits[0];
                if(type.equals("ROBBERY")){
                    r.insert(ts, ts + window);
                    rPairs.add(new Pair<>(ts, line));
                }else if(type.equals("BATTERY")){
                    bPairs.add(new Pair<>(ts, line));
                }else if(type.equals("MOTOR_VEHICLE_THEFT")){
                    m.insert(ts - window, ts);
                    mPairs.add(new Pair<>(ts, line));
                    mJoinMap.computeIfAbsent(ts / window, k -> new HashSet<>()).add(splits[3]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int cnt = 0;
        List<Pair<Long, String>> newBPairs = new ArrayList<>(1024);
        for(Pair<Long, String> p : bPairs){
            long ts = p.getKey();
            String value = p.getValue();
            String key = value.split(",")[3];
            long windowId= ts / window;
            boolean find = false;
            if(mJoinMap.containsKey(windowId)){
                if(mJoinMap.get(windowId).contains(key)){
                    b.insert(ts - window, ts + window);
                    find = true;
                }
            }

            if(mJoinMap.containsKey(windowId + 1)){
                if(mJoinMap.get(windowId + 1).contains(key)){
                    b.insert(ts - window, ts + window);
                    find = true;
                }
            }

            if(find){
                newBPairs.add(p);
                cnt++;
            }
        }
        bPairs = newBPairs;


        m.intersect(r);
        m.intersect(b);

        System.out.println("b join m -> cnt: " + cnt);

        int r_cnt = 0;
        for(Pair<Long, String> p : rPairs){
            if(m.contains(p.getKey())){
                r_cnt++;
                //System.out.println(p.getValue());
            }
        }

        int b_cnt = 0;
        for(Pair<Long, String> p : bPairs){
            if(m.contains(p.getKey())){
                b_cnt++;
            }
        }

        int m_cnt = 0;
        for(Pair<Long, String> p : mPairs){
            if(m.contains(p.getKey())){
                m_cnt++;
            }
        }

        System.out.println("r num: " + r_cnt);
        System.out.println("b num: " + b_cnt);
        System.out.println("m num: " + m_cnt);
        System.out.println("sum: " + (r_cnt + b_cnt + m_cnt));



    }
}
/*
7910 vs. 6692

MOTOR_VEHICLE_THEFT,12977412,522,5,41.688304948,-87.630185403,1675866600
MOTOR_VEHICLE_THEFT,12977905,412,4,41.737133861,-87.570271751,1675866600
ROBBERY,12977237,1932,19,41.927734308,-87.65611194,1675866600

&&&&&&&&
BATTERY,12977330,1124,11,41.880583409,-87.702959529,1675866600
BATTERY,12977274,334,3,41.763664133,-87.561402361,1675866900
BATTERY,12977267,1622,16,41.971031866,-87.777059982,1675867200
MOTOR_VEHICLE_THEFT,12977259,1234,12,41.856545391,-87.67106562,1675867440
MOTOR_VEHICLE_THEFT,12980288,1513,15,41.879783007,-87.773012281,1675867800

&&&&&&&&
MOTOR_VEHICLE_THEFT,12978202,1125,11,41.877479257,-87.692473591,1675868400
MOTOR_VEHICLE_THEFT,12977357,633,6,41.726403992,-87.61547219,1675868400
MOTOR_VEHICLE_THEFT,12977578,1013,10,41.850727019,-87.733394192,1675868400
 */

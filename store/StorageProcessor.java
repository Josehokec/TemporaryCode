package store;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 这个类测试数据：写完crimes文件需要50s,crimes文件大概8M条事件
 * 合成数据集：insertion cost 76033ms
 */
public class StorageProcessor {

    public static void storeCSVToByte(String filename, EventSchema schema){
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        //String filePath = prefixPath + "dataset" + sep + "mini_" + filename + ".csv";
        String filePath = prefixPath + "dataset" + sep + filename + ".csv";
        EventStore store = new EventStore(filename, true);

        // store records into file
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            while ((line = b.readLine()) != null) {
                byte[] record = schema.covertStringToBytes(line);
                store.insertSingleRecord(record, schema.getFixedRecordLen());
            }
            b.close();
            f.close();
            // force flush
            store.forceFlush();
        }catch (IOException e) {
            System.out.println(e.getMessage());
        }

//        MappedByteBuffer buffer = store.getMappedBuffer(0, store.getFileSize());
//        String[] headers = {"PRIMARY_TYPE", "ID", "BEAT", "DISTRICT", "LATITUDE", "LONGITUDE", "TIMESTAMP"};
//        System.out.printf("%-32s %-10s %-10s %-6s %-16s %-16s %-12s%n", headers[0], headers[1], headers[2], headers[3], headers[4], headers[5], headers[6]);
//        //max page: 128351
//        int pagePos = 0;
//        for(int i = 0; i < 3; ++i){
//            String primaryType = (String) schema.getColumnValue("PRIMARY_TYPE", buffer, pagePos);
//            int id = (int) schema.getColumnValue("ID", buffer, pagePos);
//            int beat = (int) schema.getColumnValue("BEAT", buffer, pagePos);
//            int district = (int) schema.getColumnValue("DISTRICT", buffer, pagePos);
//            double latitude = (double) schema.getColumnValue("LATITUDE", buffer, pagePos);
//            double longitude = (double) schema.getColumnValue("LONGITUDE", buffer, pagePos);
//            long timestamp = (long) schema.getColumnValue("TIMESTAMP", buffer, pagePos);
//            System.out.printf("%-32s %-10s %-10s %-6s %-16s %-16s %-12s%n", primaryType, id, beat, district, latitude, longitude, timestamp);
//            pagePos += schema.getFixedRecordLen();
//        }
    }

    public static void initialSynthetic(){
        String sql = "create table SYNTHETIC(" +
                "       type VARCHAR(8)," +
                "       id int," +
                "       a1 int," +
                "       a2 double," +
                "       a3 VARCHAR(8)," +
                "       a4 VARCHAR(8)," +
                "       timestamp long);";
        String filename = "synthetic";
        EventSchema schema = new EventSchema(sql);

        System.out.println("start writing csv file...");
        long startTime = System.currentTimeMillis();
        StorageProcessor.storeCSVToByte(filename, schema);
        long endTime = System.currentTimeMillis();
        System.out.println("insertion cost " + (endTime - startTime) + "ms");
        System.out.println("end writing csv file...");
    }

    public static void initialCrimes(){
        String sql = "create table CRIMES(" +
                "       type VARCHAR(32)," +
                "       id int," +
                "       beat int," +
                "       district int," +
                "       latitude double," +
                "       longitude double," +
                "       eventTime long);";

        //String filename = "crimes";
        EventSchema schema = new EventSchema(sql);
        //EventStore.pageSize = 512;            // only used for debugging

//        System.out.println("start writing csv file...");
//        long startTime = System.currentTimeMillis();
//        StorageProcessor.storeCSVToByte(filename, schema);
//        long endTime = System.currentTimeMillis();
//        System.out.println("insertion cost " + (endTime - startTime) + "ms");
//        System.out.println("end writing csv file...");

        List<String> ips1 = new ArrayList<>();
        ips1.add("V1.BEAT >= 1900");
        ips1.add("V1.BEAT <= 2000");
        ips1.add("V1.TYPE  = 'ROBBERY'");

        List<String> ips2 = new ArrayList<>();
        ips2.add("V2.TYPE = 'BATTERY'");

        List<String> ips3 = new ArrayList<>();
        ips3.add("V3.TYPE = 'MOTOR_VEHICLE_THEFT'");

        Map<String, List<String>> ipStringMap = new HashMap<>();
        ipStringMap.put("V1", ips1);
        ipStringMap.put("V2", ips2);
        ipStringMap.put("V3", ips3);

        FullScan fullScan = new FullScan("CRIMES");
        long startScan = System.currentTimeMillis();
        EventCache cache = fullScan.scanBasedVarName(ipStringMap);
        System.out.println(cache.getCardinality());
        long endScan = System.currentTimeMillis();
        System.out.println("scan cost " + (endScan - startScan) + "ms");

//        //cache.print();
//        System.out.println(cache.getCardinality());
//        // {V1=86981, V2=26143, V3=36191}
//
//        long startFilter = System.currentTimeMillis();
//        long window = 1800;
//        List<byte[]> vList = cache.getVarRecords("V1");
//        ReplayIntervals replayIntervals1 = new ReplayIntervals(8 * 1024);
//        for(byte[] v : vList){
//            long ts = schema.getTimestamp(v);
//            replayIntervals1.insert(ts, ts + 1800);
//        }
//        System.out.println("replay interval size: " + replayIntervals1.serialize().capacity() + " bytes");
//
//        int keyNum = replayIntervals1.getKeyNumber(window);
//        ShrinkFilterUltra sfu = new ShrinkFilterUltra.Builder(keyNum).build();
//        for(ReplayIntervals.TimeInterval interval : replayIntervals1.getIntervals()){
//            sfu.insert(interval.getStartTime(), interval.getEndTime(), window);
//        }
//        System.out.println("shrink filter size: " + sfu.serialize().capacity());
//        ByteBuffer updatedMarkerBuff = cache.updatePointers("V3", window, 1, sfu);
//        UpdatedMarkers updatedMarkers = UpdatedMarkers.deserialize(updatedMarkerBuff);
//        sfu.rebuild(updatedMarkers);
//        System.out.println("filtered size: " + updatedMarkers.getFilteredEventNum());
//        updatedMarkerBuff = cache.updatePointers("V2", window, 2, sfu);
//        updatedMarkers = UpdatedMarkers.deserialize(updatedMarkerBuff);
//        sfu.rebuild(updatedMarkers);
//        ByteBuffer ans = cache.getRecords(window, sfu);
//
//
////        ByteBuffer replayIntervalBuff = cache.updatePointers("V3", window, 1, replayIntervals1);
////        ReplayIntervals replayIntervals2 = ReplayIntervals.deserialize(replayIntervalBuff);
////        replayIntervalBuff = cache.updatePointers("V2", window, 2, replayIntervals2);
////        ReplayIntervals replayIntervals3 = ReplayIntervals.deserialize(replayIntervalBuff);
////        ByteBuffer ans = cache.getRecords(replayIntervals3);
//        long endFilter = System.currentTimeMillis();
//        System.out.println("filter cost " + (endFilter - startFilter) + "ms");
//        System.out.println("res number: " + ans.capacity() / schema.getFixedRecordLen());
//        System.out.println(cache.getCardinality());
    }

    public static void main(String[] args){
        //initialSynthetic();
        initialCrimes();
    }
}



/*
"SELECT * FROM Crimes MATCH_RECOGNIZE(\n" +
                    "    ORDER BY timestamp\n" +
                    "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                    "    ONE ROW PER MATCH\n" +
                    "    AFTER MATCH SKIP TO NEXT ROW \n" +
                    "    PATTERN (A N1+? B N2+? C) WITHIN INTERVAL '30' MINUTER\n" +
                    "    DEFINE \n" +
                    "        A AS A.primary_type = 'ROBBERY' AND A.BEAT >= 1988 AND A.BEAT <= 2000, \n" +
                    "        B AS B.primary_type = 'BATTERY' AND B.BEAT >= 1988 AND B.BEAT <= 2000, \n" +
                    "        C AS C.primary_type = 'MOTOR_VEHICLE_THEFT' \n" +
                    ") MR;";
 */


package computer;

import filter.ShrinkFilter;
import filter.ShrinkFilterUltra;
import filter.UpdatedMarkers;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import query.QueryParse;
import rpc.iface.BitmapRPC;
import rpc.iface.FilterRPC;
import rpc.iface.FilterUltraRPC;
import rpc.IntervalSetService;
import store.EventSchema;
import utils.ReplayIntervals;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

// 这个地方相应查询请求
// 1.解析查询
// 2.获取ShrinkFilter
// 3.一次过滤不相关事件
// 4.拉去所有有关事件
// 5.match
public class QueryProcessorDemo {

    public static void filterBasedMethod(String querySQL){
        QueryParse query = new QueryParse(querySQL);
        String tableName = query.getTableName();
        System.out.println(query.getIpStringMap());
        Map<String, List<String>> dpMap = query.getDpMap();
        System.out.println(query.getDpMap());

        TTransport transport = null;
        int maxMassageLen = 512 * 1024 * 1024 + 100;    // 512 MB
        int recursionLimit = 64;
        TConfiguration conf = new TConfiguration(maxMassageLen, maxMassageLen, recursionLimit);
        //String storageNodeIp = "114.212.189.13";
        String storageNodeIp = "localhost";
        int port = 9090;
        int timeout = 0;//20_000;    // get response within 20s

        try(TSocket socket = new TSocket(conf, storageNodeIp, port, timeout)){
            transport = new TFramedTransport(socket, maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            FilterRPC.Client client = new FilterRPC.Client(protocol);
            transport.open();
            Map<String, Integer> varEventNumMap = client.initial(tableName, query.getIpStringMap());

            System.out.println(varEventNumMap);
            List<Pair<String, Integer>> sortedVarList = new ArrayList<>(varEventNumMap.size());
            for (String varName : varEventNumMap.keySet()) {
                int scale = (query.headTailMarker(varName) == 2) ? 2 : 1;
                int approxWindowIdNum = varEventNumMap.get(varName) * scale;
                Pair<String, Integer> pair = new Pair<>(varName, approxWindowIdNum);
                sortedVarList.add(pair);
            }
            // 按值排序条目
            sortedVarList.sort(Comparator.comparingLong(Pair::getValue));

            // 得到第一个变量对应的事件间隙列表
            long window = query.getWindow();
            String variableName = sortedVarList.get(0).getKey();
            Set<String> hasVisitVarNames = new HashSet<>(8);
            hasVisitVarNames.add(variableName);
            int headTailMarker1 = query.headTailMarker(variableName);
            // 注意多个client时需要合并
            ByteBuffer intervalBuffer = client.getReplayIntervals(variableName, window, headTailMarker1);
            System.out.println("原始replay interval大小：" +intervalBuffer.capacity()/1000 + "KB");
            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(intervalBuffer);

            long insertStartTime = System.currentTimeMillis();
            int keyNum = replayIntervals.getKeyNumber(window);

            ShrinkFilter sf = new ShrinkFilter.Builder(keyNum).build();
            List<ReplayIntervals.TimeInterval> intervals = replayIntervals.getIntervals();
            for(ReplayIntervals.TimeInterval interval : intervals) {
                sf.insert(interval.getStartTime(), interval.getEndTime(), window);
            }
            double windowCnt = sf.getApproximateWindowNum();
            ByteBuffer sfBuffer = sf.serialize();
            long insertEndTime = System.currentTimeMillis();
            System.out.println("keyNum: " + keyNum);
            System.out.println("build shrink filter cost: " + (insertEndTime - insertStartTime) + "ms");

            int varNum = sortedVarList.size();

            // 把第一个变量对应的shrink filter构建好了

            for(int i = 1; i < varNum; ++i){
                String varName = sortedVarList.get(i).getKey();

                // 判断有没有依赖谓词
                Map<String, List<String>> sendDPMap = new HashMap<>(8);
                Map<String, Integer> eventNumMap = new HashMap<>(8);
                Map<String, Boolean> previousOrNext = new HashMap<>(8);
                boolean hasDP = false;

                for(String hasVisitVarName : hasVisitVarNames){
                    String key = hasVisitVarName.compareTo(varName) < 0 ? (hasVisitVarName + "-" + varName) : (varName + "-" + hasVisitVarName);
                    if(dpMap.containsKey(key)){
                        hasDP = true;
                        // Map<String, List<String>> dependentPredicateMap,
                        sendDPMap.put(hasVisitVarName, dpMap.get(key));
                        // 估算一下要开的大小
                        double curWindowCnt = sf.getApproximateWindowNum();

                        System.out.println("curWindowCnt: " + curWindowCnt);
                        System.out.println("windowCnt: " + windowCnt);
                        System.out.println("variable name: " + hasVisitVarName + " number of events: " + varEventNumMap.get(hasVisitVarName));

                        eventNumMap.put(hasVisitVarName, (int) (varEventNumMap.get(hasVisitVarName) * (curWindowCnt/windowCnt)));
                        previousOrNext.put(hasVisitVarName, query.compareSequence(hasVisitVarName, varName));
                    }
                }

                int headTailMarker = query.headTailMarker(varName);
                if(hasDP){
                    Map<String, ByteBuffer> bfBufferMap = client.getBF4EQJoin(varName, window, sendDPMap, eventNumMap, sfBuffer);
                    sfBuffer = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                }else{
                    System.out.println("变量：'" +varName + "'发送shirnk filter大小：" + sfBuffer.capacity()/1000 + "KB");
                    sfBuffer = client.windowFilter(varName, window, headTailMarker, sfBuffer);
                    System.out.println("变量：'" +varName + "'接收shirnk filter大小：" + sfBuffer.capacity()/1000 + "KB");
                }

                // 多个节点需要合并 multiple nodes, then we need to merge
                long startUpdateTime = System.currentTimeMillis();
                //ShrinkFilter updatedShrinkFilter = ShrinkFilter.deserialize(sfBuffer);
                //updatedShrinkFilter.rebuild();
                //sfBuffer = updatedShrinkFilter.serialize();
                sf  = ShrinkFilter.deserialize(sfBuffer);
                sf.rebuild();
                sfBuffer = sf.serialize();
                long endUpdateTime = System.currentTimeMillis();
                System.out.println("rebuild shrink filter cost: " + (endUpdateTime - startUpdateTime) + "ms");
                hasVisitVarNames.add(varName);
            }

            System.out.println("最后发送shirnk filter大小：" + sfBuffer.capacity()/1000 + "KB");
            ByteBuffer recordBuffer = client.getAllFilteredEvents(window, sfBuffer);
            // 数据长度 * 数据个数
            EventSchema schema = EventSchema.getEventSchema(tableName);
            int recordLen = schema.getFixedRecordLen();
            int capacity = recordBuffer.capacity();
            int recordNum = capacity / recordLen;
            System.out.println("record number: " + recordNum);

            // write file
//            String sep = File.separator;
//            String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
//            String outputFilePath = prefixPath + "output" + sep + "crimes_q1" + ShrinkFilter.SHRINK_FILTER_TABLE_MODE + ".txt";
//            PrintStream printStream = new PrintStream(outputFilePath);
//            System.setOut(printStream);
//            for(int i = 0; i < recordNum; ++i){
//                byte[] record = new byte[recordLen];
//                recordBuffer.get(record);
//                System.out.println(schema.getRecordStr(record));
//            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            transport.close();
        }
    }

    public static void filterUltraBasedMethod(String querySQL){
        QueryParse query = new QueryParse(querySQL);
        String tableName = query.getTableName();
        System.out.println(query.getIpStringMap());
        Map<String, List<String>> dpMap = query.getDpMap();
        System.out.println(query.getDpMap());

        TTransport transport = null;
        int maxMassageLen = 512 * 1024 * 1024 + 100;    // 512 MB
        int recursionLimit = 64;
        TConfiguration conf = new TConfiguration(maxMassageLen, maxMassageLen, recursionLimit);
        //String storageNodeIp = "114.212.189.13";
        String storageNodeIp = "localhost";
        int port = 9090;
        int timeout = 0;//20_000;    // get response within 20s

        try(TSocket socket = new TSocket(conf, storageNodeIp, port, timeout)){
            transport = new TFramedTransport(socket, maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            FilterUltraRPC.Client client = new FilterUltraRPC.Client(protocol);
            transport.open();
            Map<String, Integer> varEventNumMap = client.initial(tableName, query.getIpStringMap());

            System.out.println(varEventNumMap);
            List<Pair<String, Integer>> sortedVarList = new ArrayList<>(varEventNumMap.size());
            for (String varName : varEventNumMap.keySet()) {
                int scale = (query.headTailMarker(varName) == 2) ? 2 : 1;
                int approxWindowIdNum = varEventNumMap.get(varName) * scale;
                Pair<String, Integer> pair = new Pair<>(varName, approxWindowIdNum);
                sortedVarList.add(pair);
            }

            Map<String, Pair<Integer, Double>> filteredEventNum = new HashMap<>();

            // 按值排序
            sortedVarList.sort(Comparator.comparingLong(Pair::getValue));

            // 得到第一个变量对应的事件间隙列表
            long window = query.getWindow();
            String minVarName = sortedVarList.get(0).getKey();
            Set<String> hasVisitVarNames = new HashSet<>(8);
            hasVisitVarNames.add(minVarName);
            int headTailMarker1 = query.headTailMarker(minVarName);
            // 注意多个client时需要合并
            ByteBuffer intervalBuffer = client.getReplayIntervals(minVarName, window, headTailMarker1);
            System.out.println("原始replay interval大小：" +intervalBuffer.capacity() / 1000 + "KB");
            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(intervalBuffer);

            long insertStartTime = System.currentTimeMillis();
            int keyNum = replayIntervals.getKeyNumber(window);

            ShrinkFilterUltra sfu = new ShrinkFilterUltra.Builder(keyNum).build();
            List<ReplayIntervals.TimeInterval> intervals = replayIntervals.getIntervals();
            for(ReplayIntervals.TimeInterval interval : intervals) {
                sfu.insert(interval.getStartTime(), interval.getEndTime(), window);
            }
            double windowCnt = sfu.getApproximateWindowNum();
            // 拿到windowCnt之后更新filteredEventNum
            filteredEventNum.put(minVarName, new Pair<>(sortedVarList.get(0).getValue(), windowCnt));
            ByteBuffer sfuBuffer = sfu.serialize();

            long insertEndTime = System.currentTimeMillis();
            System.out.println("keyNum: " + keyNum);
            System.out.println("build shrink filter cost: " + (insertEndTime - insertStartTime) + "ms");

            // 把第一个变量对应的shrink filter构建好了
            int varNum = sortedVarList.size();
            ByteBuffer updatedMarkersBuffer = null;
            for(int i = 1; i < varNum; ++i){
                String varName = sortedVarList.get(i).getKey();
                // 判断有没有依赖谓词
                Map<String, List<String>> sendDPMap = new HashMap<>(8);
                Map<String, Integer> eventNumMap = new HashMap<>(8);
                Map<String, Boolean> previousOrNext = new HashMap<>(8);
                boolean hasDP = false;

                for(String hasVisitVarName : hasVisitVarNames){
                    String key = hasVisitVarName.compareTo(varName) < 0 ? (hasVisitVarName + "-" + varName) : (varName + "-" + hasVisitVarName);
                    if(dpMap.containsKey(key)){
                        hasDP = true;
                        // Map<String, List<String>> dependentPredicateMap
                        sendDPMap.put(hasVisitVarName, dpMap.get(key));
                        // 估算一下要开的大小
                        Pair<Integer, Double> p = filteredEventNum.get(hasVisitVarName);
                        double estimateNum = p.getValue() == windowCnt ? p.getKey() : p.getKey() * windowCnt / p.getValue();
                        System.out.println("variable: " + hasVisitVarName + " estimated size: " + estimateNum);
                        // estimateNum vs. 10, 000
                        eventNumMap.put(hasVisitVarName, CalculateKeyNum.calCrimesDistrict(estimateNum, windowCnt));
                        previousOrNext.put(hasVisitVarName, query.compareSequence(hasVisitVarName, varName));
                    }
                }

                int headTailMarker = query.headTailMarker(varName);
                if(hasDP){
                    ByteBuffer bb = i == 1 ? sfuBuffer : updatedMarkersBuffer;
                    // 先过滤一波 得到bloom filters
                    Map<String, ByteBuffer> bfBufferMap = client.getBF4EQJoin(varName, window, sendDPMap, eventNumMap, bb);

                    //LockFreeBloomFilter cBF = LockFreeBloomFilter.deserialize(bfBufferMap.get("C"));
                    //boolean contains = cBF.containsWithWindow(String.valueOf(11), 1675868400/1800);
                    //System.out.println("contains? " +contains);
                    //Map<String, ByteBuffer> copiedBfBufferMap = new HashMap<>(4);
                    //copiedBfBufferMap.put("C", cBF.serialize());

                    updatedMarkersBuffer = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                }else{
                    ByteBuffer bb = i == 1 ? sfuBuffer : updatedMarkersBuffer;
                    System.out.println(varName + " send shrink filter/updated markers size：" + bb.capacity()/1000 + "KB");
                    updatedMarkersBuffer = client.windowFilter(varName, window, headTailMarker, bb);
                    System.out.println(varName + " receive updated markers size：" + updatedMarkersBuffer.capacity()/1000 + "KB");
                }

                UpdatedMarkers updatedMarkers = UpdatedMarkers.deserialize(updatedMarkersBuffer);
                sfu.rebuild(updatedMarkers);
                // 更新windowCnt
                windowCnt = sfu.getApproximateWindowNum();
                // 记得多个存储节点的时候累加
                filteredEventNum.put(varName, new Pair<>(updatedMarkers.getFilteredEventNum(), windowCnt));
                // 重新序列化
                updatedMarkersBuffer = updatedMarkers.serialize(0);
                // 多个节点需要合并 multiple nodes, then we need to merge
                // 记得合并UpdatedMarkers
                hasVisitVarNames.add(varName);
            }

            System.out.println("final updated markers：" + updatedMarkersBuffer.capacity() / 1000 + "KB");
            ByteBuffer recordBuffer = client.getAllFilteredEvents(window, updatedMarkersBuffer);
            // 数据长度 * 数据个数
            EventSchema schema = EventSchema.getEventSchema(tableName);
            int recordLen = schema.getFixedRecordLen();
            int capacity = recordBuffer.remaining();
            int recordNum = capacity / recordLen;
            System.out.println("record number: " + recordNum);

            // write file
            String sep = File.separator;
            String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
            String outputFilePath = prefixPath + "output" + sep + "crimes_q1_ultra_join.txt";
            PrintStream printStream = new PrintStream(outputFilePath);
            System.setOut(printStream);
            for(int i = 0; i < recordNum; ++i){
                byte[] record = new byte[recordLen];
                recordBuffer.get(record);
                System.out.println(schema.getRecordStr(record));
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            transport.close();
        }
    }

    public static void bitmapBasedMethod(String querySQL){
        TTransport transport = null;
        int maxMassageLen = 512 * 1024 * 1024 + 100;
        int recursionLimit = 64;
        TConfiguration conf = new TConfiguration(maxMassageLen, maxMassageLen, recursionLimit);
        //String storageNodeIp = "114.212.189.13";
        String storageNodeIp = "localhost";
        int port = 9090;
        int timeout = 20_000;    // get response within 20s

        try(TSocket socket = new TSocket(conf, storageNodeIp, port, timeout)){
            transport = new TFramedTransport(socket, maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            BitmapRPC.Client client = new BitmapRPC.Client(protocol);
            transport.open();

            QueryParse query = new QueryParse(querySQL);
            String tableName = query.getTableName();
            // System.out.println(query.getIpStringMap());
            Map<String, Integer> varEventNumMap = client.initial(tableName, query.getIpStringMap());

            System.out.println(varEventNumMap);
            List<Pair<String, Integer>> sortedVarList = new ArrayList<>(varEventNumMap.size());
            for (String varName : varEventNumMap.keySet()) {
                int scale = (query.headTailMarker(varName) == 2) ? 2 : 1;
                int approxWindowIdNum = varEventNumMap.get(varName) * scale;
                Pair<String, Integer> pair = new Pair<>(varName, approxWindowIdNum);
                sortedVarList.add(pair);
            }
            // 按值排序条目
            sortedVarList.sort(Comparator.comparingLong(Pair::getValue));

            // 得到第一个变量对应的事件间隙列表
            long window = query.getWindow();
            String variableName = sortedVarList.get(0).getKey();
            int headTailMarker1 = query.headTailMarker(variableName);
            // 注意多个位图需要合并 multiple bitmaps need to merge
            ByteBuffer bitmap = client.getReplayIntervals(variableName, window, headTailMarker1);


            int varNum = sortedVarList.size();
            for(int i = 1; i < varNum; ++i){
                String varName = sortedVarList.get(i).getKey();
                int headTailMarker = query.headTailMarker(varName);
                bitmap = client.windowFilter(varName, window, headTailMarker, bitmap);
                // 多个存储节点记得合并
            }

            ByteBuffer recordBuffer = client.getAllFilteredEvents(window, bitmap);
            // 数据长度 * 数据个数
            EventSchema schema = EventSchema.getEventSchema(tableName);
            int recordLen = schema.getFixedRecordLen();
            int capacity = recordBuffer.capacity();
            int recordNum = capacity / recordLen;
            //....
            System.out.println("record number: " + recordNum);

            // 估算开多大的bloom filter空间
            // 目前我们可以知道v_i对应多少个key和多少个window
            // 则我们可以算出过滤后有多少个key
            // 另外，distinct value (DIV) 可能很少，我们看 key/windowNum 是不是大于distinct value，是的话则
            // 认为key的数量可以取为：DIV * windowNum
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            transport.close();
        }
    }

    public static void intervalSetBasedMethod(String querySQL){
        TTransport transport = null;
        int maxMassageLen = 512 * 1024 * 1024 + 100;
        int recursionLimit = 64;
        TConfiguration conf = new TConfiguration(maxMassageLen, maxMassageLen, recursionLimit);
        //String storageNodeIp = "114.212.189.13";
        String storageNodeIp = "localhost";
        int port = 9090;
        int timeout = 20_000;    // get response within 20s

        try(TSocket socket = new TSocket(conf, storageNodeIp, port, timeout)){
            transport = new TFramedTransport(socket, maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            IntervalSetService.Client client = new IntervalSetService.Client(protocol);
            transport.open();

            QueryParse query = new QueryParse(querySQL);
            String tableName = query.getTableName();

            // System.out.println(query.getIpStringMap());
            // 这个统一的不计算在内
            Map<String, Integer> varEventNumMap = client.initial(tableName, query.getIpStringMap());

            System.out.println(varEventNumMap);
            List<Pair<String, Integer>> sortedVarList = new ArrayList<>(varEventNumMap.size());
            for (String varName : varEventNumMap.keySet()) {
                int scale = (query.headTailMarker(varName) == 2) ? 2 : 1;
                int approxWindowIdNum = varEventNumMap.get(varName) * scale;
                Pair<String, Integer> pair = new Pair<>(varName, approxWindowIdNum);
                sortedVarList.add(pair);
            }
            // 按值排序条目
            sortedVarList.sort(Comparator.comparingLong(Pair::getValue));

            // 得到第一个变量对应的事件间隙列表
            long window = query.getWindow();
            String variableName = sortedVarList.get(0).getKey();
            int headTailMarker1 = query.headTailMarker(variableName);
            // 注意如果有多个replays intervals则需要合并
            ByteBuffer intervalBuffer = client.generateIntervals(variableName, window, headTailMarker1);
            System.out.println("接收replay interval大小：" +intervalBuffer.capacity()/1000 + "KB");

            int varNum = sortedVarList.size();
            for(int i = 1; i < varNum; ++i){
                String varName = sortedVarList.get(i).getKey();
                int headTailMarker = query.headTailMarker(varName);
                System.out.println("变量：'"+ varName + "'发送replay intervals大小：" + intervalBuffer.capacity()/1000 + "KB");
                intervalBuffer = client.filterBasedWindow(varName, window, headTailMarker, intervalBuffer);
                // 多个存储节点记得合并
                System.out.println("变量：'"+ varName + "'接收replay intervals大小：" + intervalBuffer.capacity()/1000 + "KB");
            }

            //最后发送shirnk filter大小：32KB
            System.out.println("最后发送replay intervals大小：：" + intervalBuffer.capacity()/1000 + "KB");
            ByteBuffer recordBuffer = client.getAllFilteredEvents(intervalBuffer);
            // 数据长度 * 数据个数
            EventSchema schema = EventSchema.getEventSchema(tableName);
            int recordLen = schema.getFixedRecordLen();
            int capacity = recordBuffer.capacity();
            int recordNum = capacity / recordLen;
            System.out.println("record number: " + recordNum);
            // write file
//            String sep = File.separator;
//            String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
//            String outputFilePath = prefixPath + "output" + sep + "crimes_q1_interval" + ".txt";
//            PrintStream printStream = new PrintStream(outputFilePath);
//            System.setOut(printStream);
//            for(int i = 0; i < recordNum; ++i){
//                byte[] record = new byte[recordLen];
//                recordBuffer.get(record);
//                System.out.println(schema.getRecordStr(record));
//            }

        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            transport.close();
        }
    }

    public static void main(String[] args) {
        String querySQL =
                "SELECT * FROM Crimes MATCH_RECOGNIZE(\n" +
                        "    ORDER BY timestamp\n" +
                        "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                        "    ONE ROW PER MATCH\n" +
                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
                        "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTER\n" +
                        "    DEFINE \n" +
                        "        A AS A.primary_type = 'ROBBERY' AND A.beat >= 1900 AND A.beat <= 2000, \n" +
                        "        B AS B.primary_type = 'BATTERY', \n" +
                        "        C AS C.primary_type = 'MOTOR_VEHICLE_THEFT' AND C.DISTRICT = B.DISTRICT \n" + // AND C.DISTRICT = B.DISTRICT
                        ") MR;";


        //filterBasedMethod(querySQL);
        filterUltraBasedMethod(querySQL);
        //bitmapBasedMethod(querySQL);
        // record number: 29689
        //intervalSetBasedMethod(querySQL);
    }

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
}

/*
debug codes
// 每个变量对应的事件数量
for(int i = 0; i < 10; ++i){
    long start = System.currentTimeMillis();
    ByteBuffer buf = client.generateHashTable4NonEqualJoin("test", -1, "test");
    long end = System.currentTimeMillis();
    // "cost: " +
    System.out.println((end - start));
}

 */
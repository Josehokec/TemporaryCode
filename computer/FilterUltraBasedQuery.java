package computer;

import event.CrimesEvent;
import filter.ShrinkFilterUltra;
import filter.UpdatedMarkers;
import org.apache.flink.table.api.*;
import plan.GeneratedPlan;
import query.QueryParse;
import plan.Plan;
import rpc.iface.FilterUltraRPC;
import store.EventSchema;
import utils.ReplayIntervals;
import utils.Pair;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

public class FilterUltraBasedQuery {
    public static int maxMassageLen = 512 * 1024 * 1024 + 100;
    public static int recursionLimit = 64;

    static byte[] orByteArrays(byte[] array1, byte[] array2) {
        int length = Math.min(array1.length, array2.length);
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (array1[i] | array2[i]);
        }
        return result;
    }

    static class InitialThread extends Thread {
        private final FilterUltraRPC.Client client;
        private final String tableName;
        // ip: independent predicate
        private final Map<String, List<String>> ipMap;
        private Map<String, Integer> varEventNumMap;

        public InitialThread(FilterUltraRPC.Client client, String tableName, Map<String, List<String>> ipMap){
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
        }

        public Map<String, Integer> getVarEventNumMap(){
            return varEventNumMap;
        }

        @Override
        public void run() {
            try {
                varEventNumMap = client.initial(tableName, ipMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class GenerateIntervalThread extends Thread{
        private final FilterUltraRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        //ByteBuffer intervalBuffer = client.generateIntervals(variableName, window, headTailMarker1);

        public GenerateIntervalThread(FilterUltraRPC.Client client, String varName, long window, int headTailMarker){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
        }

        public ReplayIntervals getReplayIntervals(){
            return ri;
        }

        @Override
        public void run() {
            try {
                ByteBuffer buffer = client.getReplayIntervals(varName, window, headTailMarker);
                ri = ReplayIntervals.deserialize(buffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class WindowFilterThread extends Thread{
        private final FilterUltraRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer binaryShrinkFilter;
        private UpdatedMarkers updateMarkers;
        // client.filterBasedWindow(varName, window, headTailMarker, sfBuffer);

        public WindowFilterThread(FilterUltraRPC.Client client, String varName, long window, int headTailMarker, ByteBuffer binaryShrinkFilter){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.binaryShrinkFilter = binaryShrinkFilter;
        }

        public UpdatedMarkers getUpdateMarkers(){
            return updateMarkers;
        }

        @Override
        public void run() {
            try{
                ByteBuffer binaryUpdatedMarkers  = client.windowFilter(varName, window, headTailMarker, binaryShrinkFilter);
                updateMarkers = UpdatedMarkers.deserialize(binaryUpdatedMarkers);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class RequestBloomFilterThread extends Thread{
        private final FilterUltraRPC.Client client;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, Integer> eventNumMap;
        private final ByteBuffer bb;
        Map<String, ByteBuffer> bfBufferMap;

        public Map<String, ByteBuffer> getBfBufferMap(){
            return bfBufferMap;
        }

        public RequestBloomFilterThread(FilterUltraRPC.Client client, String varName, long window, Map<String, List<String>> sendDPMap, Map<String, Integer> eventNumMap, ByteBuffer bb){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.sendDPMap = sendDPMap;
            this.eventNumMap = eventNumMap;
            this.bb = bb;
        }

        @Override
        public void run() {
            try{
                bfBufferMap = client.getBF4EQJoin(varName, window, sendDPMap, eventNumMap, bb);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class EqJoinFilterThread extends Thread{
        private final FilterUltraRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, ByteBuffer> bfBufferMap;
        private UpdatedMarkers updateMarkers;

        public EqJoinFilterThread(FilterUltraRPC.Client client, String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> sendDPMap, Map<String, ByteBuffer> bfBufferMap){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.previousOrNext = previousOrNext;
            this.sendDPMap = sendDPMap;
            this.bfBufferMap = bfBufferMap;
        }

        public UpdatedMarkers getUpdateMarkers(){
            return updateMarkers;
        }

        @Override
        public void run() {
            try{
                ByteBuffer binaryUpdatedMarkers  = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                updateMarkers = UpdatedMarkers.deserialize(binaryUpdatedMarkers);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class PullEventThread extends Thread{
        private final FilterUltraRPC.Client client;
        private final long window;
        private final ByteBuffer updatedMarkersBuffer;
        private final int recordLen;
        private List<byte[]> events;
        public PullEventThread(FilterUltraRPC.Client client, long window, ByteBuffer updatedMarkersBuffer, int recordLen){
            this.client = client;
            this.window = window;
            this.updatedMarkersBuffer = updatedMarkersBuffer;
            this.recordLen = recordLen;
        }

        List<byte[]> getAllEvents(){
            return events;
        }

        @Override
        public void run() {
            try{
                ByteBuffer recordBuffer  = client.getAllFilteredEvents(window, updatedMarkersBuffer);
                int capacity = recordBuffer.remaining();
                int recordNum = capacity / recordLen;
                events = new ArrayList<>(recordNum);
                for(int i = 0; i < recordNum; ++i){
                    byte[] record = new byte[recordLen];
                    recordBuffer.get(record);
                    events.add(record);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * after binding connection, we start the communication phase
     * @param sql query sql
     * @param clients storage nodes
     * @return byte records
     */
    public static List<byte[]> communicate(String sql, List<FilterUltraRPC.Client> clients) {
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        Map<String, List<String>> ipMap = query.getIpStringMap();
        Map<String, List<String>> dpMap = query.getDpMap();

        long window = query.getWindow();
        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        // we need this to estimate bloom filters size
        Map<String, Pair<Integer, Double>> filteredEventNum = new HashMap<>();

        // step 1: initial (process independent predicates and return cardinality)
        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for (FilterUltraRPC.Client client : clients) {
            InitialThread initialThread = new InitialThread(client, tableName, ipMap);
            initialThread.start();
            initialThreads.add(initialThread);
        }
        for (InitialThread t : initialThreads) {
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        // aggregate number of events
        Map<String, Integer> varEventNumMap = new HashMap<>(ipMap.size() << 1);
        for(InitialThread t : initialThreads){
            Map<String, Integer> map = t.getVarEventNumMap();
            for(Map.Entry<String, Integer> entry : map.entrySet()){
                String key = entry.getKey();
                int value = entry.getValue();
                varEventNumMap.put(key, varEventNumMap.getOrDefault(key, 0) + value);
            }
        }
        // debug...
        //System.out.println("varEventNumMap: " + varEventNumMap);
        // set null to release
        initialThreads = null;

        // start loop
        ShrinkFilterUltra shrinkFilter = null;
        UpdatedMarkers updatedMarkers = null;

        Plan plan  = GeneratedPlan.basicPlan(varEventNumMap, query.getHeadVarName(), query.getTailVarName());
        List<String> steps = plan.getSteps();
        Set<String> hasProcessedVarName = new HashSet<>();
        double windowCnt = -1;
        // steps format: [variableName_{s_1}, ..., variableName_{s_n}]
        for(String varName : steps){
            if(hasProcessedVarName.isEmpty()){
                // obtain replay intervals and construct shrink filter
                List<GenerateIntervalThread> generateIntervalThreads = new ArrayList<>(nodeNum);
                for (FilterUltraRPC.Client client : clients) {
                    GenerateIntervalThread thread = new GenerateIntervalThread(client, varName, window, query.headTailMarker(varName));
                    thread.start();
                    generateIntervalThreads.add(thread);
                }
                for (GenerateIntervalThread t : generateIntervalThreads) {
                    try{
                        t.join();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                ReplayIntervals replayIntervals = null;
                // union
                for(GenerateIntervalThread t : generateIntervalThreads){
                    if(replayIntervals == null){
                        replayIntervals = t.getReplayIntervals();
                    }else{
                        ReplayIntervals ri = t.getReplayIntervals();
                        replayIntervals.union(ri);
                    }
                }
                int keyNum = replayIntervals.getKeyNumber(window);
                shrinkFilter = new ShrinkFilterUltra.Builder(keyNum).build();
                List<ReplayIntervals.TimeInterval> timeIntervals = replayIntervals.getIntervals();
                for(ReplayIntervals.TimeInterval interval : timeIntervals) {
                    shrinkFilter.insert(interval.getStartTime(), interval.getEndTime(), window);
                }
                hasProcessedVarName.add(varName);
                windowCnt = shrinkFilter.getApproximateWindowNum();
            }else{
                // we need to check dependent predicates (dp)
                Map<String, List<String>> sendDPMap = new HashMap<>(8);
                Map<String, Integer> eventNumMap = new HashMap<>(8);
                Map<String, Boolean> previousOrNext = new HashMap<>(8);
                boolean hasDP = false;
                for(String hasVisitVarName : hasProcessedVarName){
                    String key = hasVisitVarName.compareTo(varName) < 0 ? (hasVisitVarName + "-" + varName) : (varName + "-" + hasVisitVarName);
                    if(dpMap.containsKey(key)){
                        hasDP = true;
                        sendDPMap.put(hasVisitVarName, dpMap.get(key));
                        // then we need to estimate number of keys
                        Pair<Integer, Double> p = filteredEventNum.get(hasVisitVarName);
                        double estimateNum = p.getValue() == windowCnt ? p.getKey() : p.getKey() * windowCnt / p.getValue();
                        //System.out.println("variable: " + hasVisitVarName + " estimated size: " + estimateNum);
                        eventNumMap.put(hasVisitVarName, CalculateKeyNum.calCrimesDistrict(estimateNum, windowCnt));
                        previousOrNext.put(hasVisitVarName, query.compareSequence(hasVisitVarName, varName));
                    }
                }

                int headTailMarker = query.headTailMarker(varName);
                ByteBuffer bb = hasProcessedVarName.size() == 1 ? shrinkFilter.serialize() : updatedMarkers.serialize(-1);
                if(hasDP){
                    List<RequestBloomFilterThread> requestBloomFilterThreads = new ArrayList<>(nodeNum);
                    for (FilterUltraRPC.Client client : clients) {
                        RequestBloomFilterThread thread = new RequestBloomFilterThread(client, varName, window, sendDPMap, eventNumMap, bb);
                        thread.start();
                        requestBloomFilterThreads.add(thread);
                    }
                    for (RequestBloomFilterThread t : requestBloomFilterThreads){
                        try{
                            t.join();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    // merge all bloom filters
                    Map<String, byte[]> mergedBFBufferMap = new HashMap<>(8);
                    for (RequestBloomFilterThread t : requestBloomFilterThreads){
                        Map<String, ByteBuffer> bfBufferMap = t.getBfBufferMap();
                        for(String key : bfBufferMap.keySet()){
                            ByteBuffer bfBuffer = bfBufferMap.get(key);
                            // 不知道有没有bug
                            int size = bfBuffer.remaining();
                            byte[] byteArray = new byte[size];
                            bfBuffer.get(byteArray);
                            if(mergedBFBufferMap.containsKey(key)){
                                byte[] mergedBF = orByteArrays(byteArray, mergedBFBufferMap.get(key));
                                mergedBFBufferMap.put(key, mergedBF);
                            }else{
                                mergedBFBufferMap.put(key, byteArray);
                            }
                        }
                    }
                    Map<String, ByteBuffer> bfBufferMap = new HashMap<>(8);
                    for(String key : mergedBFBufferMap.keySet()){
                        ByteBuffer bfBuffer = ByteBuffer.wrap(mergedBFBufferMap.get(key));
                        bfBufferMap.put(key, bfBuffer);
                    }

                    // join filtering
                    List<EqJoinFilterThread> eqJoinFilterThreads = new ArrayList<>(nodeNum);
                    for (FilterUltraRPC.Client client : clients) {
                        EqJoinFilterThread thread = new EqJoinFilterThread(client, varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                        thread.start();
                        eqJoinFilterThreads.add(thread);
                    }
                    for (EqJoinFilterThread t : eqJoinFilterThreads){
                        try{
                            t.join();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    updatedMarkers = null;
                    for (EqJoinFilterThread t : eqJoinFilterThreads) {
                        if(updatedMarkers == null){
                            updatedMarkers = t.getUpdateMarkers();
                        }else{
                            updatedMarkers.merge(t.getUpdateMarkers());
                        }
                    }
                }else{
                    // call window filter function
                    List<WindowFilterThread> windowFilterThreads = new ArrayList<>(nodeNum);
                    for (FilterUltraRPC.Client client : clients) {
                        WindowFilterThread thread = new WindowFilterThread(client, varName, window, query.headTailMarker(varName), bb);
                        thread.start();
                        windowFilterThreads.add(thread);
                    }
                    for (WindowFilterThread t : windowFilterThreads) {
                        try{
                            t.join();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    updatedMarkers = null;
                    for (WindowFilterThread t : windowFilterThreads) {
                        if(updatedMarkers == null){
                            updatedMarkers = t.getUpdateMarkers();
                        }else{
                            updatedMarkers.merge(t.getUpdateMarkers());
                        }
                    }

                    //System.out.println("varName: " + varName + " cnt: " + updatedMarkers.getFilteredEventNum());
                }
                // if we use shrink filter, we need to call merge function
                // shrink filter ultra uses rebuild function
                shrinkFilter.rebuild(updatedMarkers);
                windowCnt = shrinkFilter.getApproximateWindowNum();
                filteredEventNum.put(varName, new Pair<>(updatedMarkers.getFilteredEventNum(), windowCnt));
                hasProcessedVarName.add(varName);
            }
        }

        int recordLen = schema.getFixedRecordLen();
        ByteBuffer bb = updatedMarkers.serialize(-1);
        List<PullEventThread> pullEventThreads = new ArrayList<>(nodeNum);
        for (FilterUltraRPC.Client client : clients) {
            PullEventThread thread = new PullEventThread(client, window, bb, recordLen);
            thread.start();
            pullEventThreads.add(thread);
        }
        for (PullEventThread t : pullEventThreads){
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        List<byte[]> byteRecords = null;
        for (PullEventThread t : pullEventThreads){
            if(byteRecords == null){
                byteRecords = t.getAllEvents();
            }else{
                byteRecords.addAll(t.getAllEvents());
            }
        }

        //System.out.println(schema.getRecordStr(byteRecords.get(0)));
        System.out.println("record size: " + byteRecords.size());
        return byteRecords;
    }

    public static void testCrimesDataset(List<FilterUltraRPC.Client> clients){
        Schema schema = Schema.newBuilder()
                .column("type", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("beat", DataTypes.INT())
                .column("district", DataTypes.INT())
                .column("latitude", DataTypes.DOUBLE())
                .column("longitude", DataTypes.DOUBLE())
                .column("eventTime", DataTypes.TIMESTAMP(3))
                .watermark("eventTime", "eventTime - INTERVAL '0' SECOND")
                .build();

        String sql =
                "SELECT * FROM Crimes MATCH_RECOGNIZE(\n" +
                "    ORDER BY eventTime\n" +
                "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                "    ONE ROW PER MATCH\n" +
                "    AFTER MATCH SKIP TO NEXT ROW \n" +
                "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '31' MINUTE \n" +
                "    DEFINE \n" +
                "        A AS A.type = 'ROBBERY' AND A.beat >= 1900 AND A.beat <= 2000, \n" +
                "        B AS B.type = 'BATTERY', \n" +
                "        C AS C.type = 'MOTOR_VEHICLE_THEFT' AND C.district = B.district \n" +  //AND C.district = B.district
                ") MR;";
        long startTime = System.currentTimeMillis();
        List<byte[]> byteRecords = communicate(sql, clients);
        List<CrimesEvent> events = byteRecords.stream().map(CrimesEvent::valueOf).collect(Collectors.toList());
        long endTime = System.currentTimeMillis();
        System.out.println("========>pull event time: " + (endTime - startTime) + "ms");
        ProcessQueryByFlink.processQuery(events, schema, sql, "Crimes");
    }

    public static void main(String[] args) throws Exception{
        TConfiguration conf = new TConfiguration(maxMassageLen, maxMassageLen, recursionLimit);
        //String storageNodeIp = "114.212.189.13";
        String[] storageNodeIps = {"localhost"};
        int[] ports = {9090};
        int nodeNum = storageNodeIps.length;
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);
        List<FilterUltraRPC.Client> clients = new ArrayList<>(nodeNum);

        for(int i = 0; i < nodeNum; i++){
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            FilterUltraRPC.Client client = new FilterUltraRPC.Client(protocol);
            // when we open, we can call related interface
            transport.open();
            clients.add(client);
            transports.add(transport);
        }

        int loop = 5;
        for(int i = 0; i < loop; i++){
            long startTime = System.currentTimeMillis();
            testCrimesDataset(clients);
            long endTime = System.currentTimeMillis();
            System.out.println("========>query cost: " + (endTime - startTime) + "ms");
            System.out.println("------------------------------------");
        }

        System.out.println("--finish--");
        // we need to close transport
        for(TTransport transport : transports){
            transport.close();
        }
    }

}

/*

a, b , c
 b.beat = c.beat
 */
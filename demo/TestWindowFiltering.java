//package demo;
//
//import event.CrimesEvent;
//import event.DataType;
//import event.Event;
//import query.IndependentPredicate;
//import query.PatternQuery;
//import query.QueryParse;
//import utils.Utils;
//
//import java.io.*;
//import java.util.*;
//
//
///**
// * 如果数据无序的话，插入效率是否会太低？
// * 如果数据有序，建议先生成replay intervals，然后整个整个replay interval插入
// */
//
//public class TestWindowFiltering {
//    public static List<Event> loadData(String filePath){
//        List<Event> events = new ArrayList<>(10_000_000);
//        try {
//            FileReader f = new FileReader(filePath);
//            BufferedReader b = new BufferedReader(f);
//            // delete first line
//            String line;
//            b.readLine();
//
//            while ((line = b.readLine()) != null) {
//                CrimesEvent eventParser = new CrimesEvent();
//                Event event = eventParser.parseString(line);
//                events.add(event);
//            }
//            b.close();
//            f.close();
//        }catch (IOException e) {
//            e.printStackTrace();
//        }
//        return events;
//    }
//
//
//    public static Map<String, List<Event>> checkVariableIndependentConstraint(Map<String, String> varTypeMap,
//                                                                              HashMap<String, List<IndependentPredicate>> varIpList,
//                                                                              List<Event> events){
//        Map<String, List<Event>> filteredEvents = new HashMap<>();
//        // to ensure not null, we need to create array in advance
//        for(String varName : varTypeMap.keySet()){
//            filteredEvents.put(varName, new ArrayList<>(1024));
//        }
//
//        for(Event event : events) {
//            for (Map.Entry<String, String> varToType : varTypeMap.entrySet()) {
//                String curVarName = varToType.getKey();
//                String curTypeName = varToType.getValue();
//                // firstly, we check event type
//                if (event.getEventType().equals(curTypeName)) {
//                    // secondly, we check independent constraints
//                    boolean satisfy = true;
//                    if (varIpList.containsKey(curVarName)) {
//                        List<IndependentPredicate> ipList = varIpList.get(curVarName);
//                        for (IndependentPredicate ip : ipList) {
//                            String columnName = ip.getAttributeName();
//                            DataType dataType = event.getDataType(columnName);
//                            // satisfy = ip.check(dataType);
//                            satisfy = ip.check(event.getAttributeValue(columnName), dataType);
//                            if(!satisfy) {
//                                break;
//                            }
//                        }
//                    }
//                    // if it is satisfied, then we store it
//                    if (satisfy) {
//                        filteredEvents.get(curVarName).add(event);
//                    }
//                }
//            }
//        }
//        return filteredEvents;
//    }
//
//    /**
//     * here we suppose without false positive rate
//     * @param events - minimum events
//     * @param queryWindow - query window
//     * @param headTailCase - 0: head variable, 1: tail variable, 2: middle variable
//     * @return - window keys
//     */
//    public static HashMap<Long, Byte> generateWindowTags(List<Event> events, long queryWindow, int headTailCase){
//        long leftOffset;
//        long rightOffset;
//        switch (headTailCase){
//            case 0:
//                leftOffset = 0;
//                rightOffset = queryWindow;
//                break;
//            case 1:
//                leftOffset = -queryWindow;
//                rightOffset = 0;
//                break;
//            default:
//                leftOffset = -queryWindow;
//                rightOffset = queryWindow;
//        }
//
//        // notice we override equals method
//        HashMap<Long, Byte> filters = new HashMap<>(4096);
//        for(Event event : events){
//            long ts = event.getTimestamp();
//            WindowTag[] windowTags = WindowTag.getWindowTags(ts + leftOffset, ts + rightOffset, queryWindow);
//            for (WindowTag windowTag : windowTags) {
//                long key = windowTag.getWindowKey();
//                byte value = windowTag.getMarker();
//                if (filters.containsKey(key)) {
//                    // we need to merge
//                    filters.put(key, (byte) (filters.get(key) | value));
//                } else {
//                    filters.put(key, value);
//                }
//            }
//        }
//
//        return filters;
//    }
//
//    /**
//     * notice that: SEQ(A v1, B v2, C v3)
//     * @param windowTags - window filter
//     * @param events - events
//     * @param queryWindow - query window
//     * @param headTailCase - 0: head variable, 1: tail variable, 2: middle variable
//     * @return - updated keys
//     */
//    public static List<Event> windowMatching(HashMap<Long, Byte> windowTags, List<Event> events, long queryWindow, int headTailCase){
//
//        List<Event> filteredEvents = new ArrayList<>(events.size());
//        // Iterator<Event> it = events.iterator(); it.hasNext(); Event event : events
//        for(Event event : events){
//            long ts = event.getTimestamp();
//            // firstly, we check timestamp whether is included in windowTags
//            boolean satisfy = false;
//            long key = ts / queryWindow;
//
//            if(windowTags.containsKey(key)){
//                long reminder = ts - key * queryWindow;
//                // shiftNum: 0, 1, 2, 3
//                long shiftNum = reminder / (queryWindow >> 2);
//                Byte value = windowTags.get(key);
//                // which
//                if(((value << shiftNum) & 0x80) != 0){
//                    satisfy = true;
//                    // set hit marker
//                    byte marker;
//                    switch (headTailCase){
//                        case 0:
//                            // [ts, ts + w]
//                            marker = windowTags.get(key);
//                            // e.g., ts = 5.4, w = 2, then shiftNum = 2
//                            // e.g., ts = 5.6, w = 2, then shiftNum = 3
//                            marker |= (0x0f >> shiftNum);           // marker = 0011
//                            // update marker
//                            windowTags.put(key, marker);
//                            // e.g., ts = 5.4, w = 2, then ts + w = 7.4, marker = 1110
//                            // e.g., ts = 5.6, w = 2, then ts + w = 7.6, marker = 1111
//                            if(windowTags.containsKey(key + 1)){
//                                marker = windowTags.get(key + 1);
//                                marker |= ((0x0f << (3 - shiftNum)) & 0x0f);
//                                windowTags.put(key + 1, marker);
//                            }
//                            break;
//                        case 1:
//                            // [ts - w, ts]
//                            marker = windowTags.get(key);
//                            // e.g., ts = 5.4, w = 2, then shiftNum = 2, marker = 1110
//                            // e.g., ts = 5.6, w = 2, then shiftNum = 3, marker = 1111
//                            marker |= ((0x0f0 >> (shiftNum + 1)) & 0x0f);
//                            windowTags.put(key, marker);
//
//                            // e.g., ts = 5.4, w = 2, then shiftNum = 2, ts - w = 3.4, marker = 0011
//                            // e.g., ts = 5.6, w = 2, then shiftNum = 3, ts - w = 3.6, marker = 0001
//                            if(windowTags.containsKey(key - 1)){
//                                marker = windowTags.get(key - 1);
//                                marker |= (0x0f >> shiftNum);
//                                windowTags.put(key - 1, marker);
//                            }
//                            break;
//                        default:
//                            // [ts - w, ts + w]
//                            marker = windowTags.get(key);
//                            marker |= 0x0f;
//                            windowTags.put(key, marker);
//
//                            // e.g., ts = 5.4, w = 2, then ts - w = 3.4, shiftNum = 2, marker = 0011
//                            // e.g., ts = 5.6, w = 2, then ts - w = 3.6, shiftNum = 3, marker = 0001
//                            if(windowTags.containsKey(key - 1)){
//                                marker = windowTags.get(key - 1);
//                                marker |= (0x0f >> shiftNum);
//                                windowTags.put(key - 1, marker);
//                            }
//
//                            // e.g., ts = 5.4, w = 2, then ts + w = 7.4, shiftNum = 2, marker = 1110
//                            // e.g., ts = 5.6, w = 2, then ts + w = 7.6, shiftNum = 3, marker = 1111
//                            if(windowTags.containsKey(key + 1)){
//                                marker = windowTags.get(key + 1);
//                                marker |= ((0x0f << (3 - shiftNum)) & 0x0f);
//                                windowTags.put(key + 1, marker);
//                            }
//                    }
//                }
//            }
//
//            if(satisfy){
//                filteredEvents.add(event);
//            }
//        }
//        //events = filteredEvents;
//        return filteredEvents;
//    }
//
//    public static void updateWindowTags(HashMap<Long, Byte> windowTags){
//        for(Iterator<Map.Entry<Long, Byte>> it = windowTags.entrySet().iterator(); it.hasNext();){
//            Map.Entry<Long, Byte> entry = it.next();
//            byte value = entry.getValue();
//            if((value & 0x0f) == 0){
//                it.remove();
//            }
//            value = (byte) (((value >> 4) & (value & 0x0f)) << 4);
//            entry.setValue(value);
//        }
//        // return windowTags;
//    }
//
//    public static void main(String[] args) throws FileNotFoundException {
//        String sep = File.separator;
//        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
//
//        // v1.beat >= 1900 AND v1.beat <= 2000 AND v1.District = v2.District AND v1.District = v3.District
//        // v1.beat <= 3000
//        String queryStr = "PATTERN SEQ(ROBBERY v1, BATTERY v2, MOTOR_VEHICLE_THEFT v3)\n" +
//                          "USING SKIP_TILL_ANY_MATCH\n" +
//                          "WHERE v1.beat >= 1900 AND v1.beat <= 2000\n" +
//                          "WITHIN 1800 units";   // here 1800 units = 30 minutes
//        PatternQuery query = QueryParse.parseQueryString(queryStr);
//        Map<String, String> varTypeMap = query.getVarTypeMap();
//        query.print();
//
//        long queryWindow = query.getQueryWindow();
//        String dataFilePath = prefixPath + "dataset" + sep + "crimes.csv";
//
//        // load data: crimes_test
//        long loadStart, loadEnd;
//        loadStart = System.currentTimeMillis();
//        List<Event> events = loadData(dataFilePath);
//        loadEnd = System.currentTimeMillis();
//        System.out.println("load data cost: " + (loadEnd - loadStart) + "ms");
//
//        long firstFilterStart = System.currentTimeMillis();
//        HashMap<String, List<IndependentPredicate>> ipMap = query.getIpMap();
//        Map<String, List<Event>> eventsMap = checkVariableIndependentConstraint(varTypeMap, ipMap, events);
//        long firstFilterEnd = System.currentTimeMillis();
//        System.out.println("filter data cost: " + (firstFilterEnd - firstFilterStart) + "ms");
//
//        long secondFilterStart = System.currentTimeMillis();
//        class Statistic implements Comparable<Statistic>{
//            final String varName;
//            final int headTailCase;
//            // when using greedy algorithm to generate suboptimal plan,
//            // we need to define a weight factor (arrival rate, selectivity, headTailCase)
//            // in our demo experiments, we use Integer rather Float to describe this weight
//            final int weight;
//            public Statistic(String varName, int headTailCase, int weight) {
//                this.varName = varName;
//                this.headTailCase = headTailCase;
//                this.weight = weight;
//            }
//
//            @Override
//            public int compareTo(Statistic o) {
//                return this.weight - o.weight;
//            }
//        }
//
//        Statistic[] statistics = new Statistic[eventsMap.size()];
//        int cnt = 0;
//        for(Map.Entry<String, List<Event>> entry : eventsMap.entrySet()){
//            List<Event> curVarEvents = entry.getValue();
//            int size = curVarEvents.size();
//            String varName = entry.getKey();
//
//            int headTailCase;
//            if(query.isOnlyLeftMostNode(varName)){
//                headTailCase = 0;
//            }else if(query.isOnlyRightMostNode(varName)){
//                headTailCase = 1;
//            }else{
//                headTailCase = 2;
//            }
//            int weight = size;
//            if(headTailCase == 0 || headTailCase == 1){
//                weight = weight >> 1;
//            }
//            statistics[cnt++] = new Statistic(varName, headTailCase, weight);
//        }
//
//        Arrays.sort(statistics);
//        HashMap<Long, Byte> windowTags = generateWindowTags(eventsMap.get(statistics[0].varName),
//                                            query.getQueryWindow(), statistics[0].headTailCase);
//
//        for(int i = 1; i < statistics.length; ++i){
//            Statistic statistic = statistics[i];
//            String curVarName = statistic.varName;
//            List<Event> curVarEvents = eventsMap.get(curVarName);
//            List<Event> filteredEvents = windowMatching(windowTags, curVarEvents, queryWindow, statistic.headTailCase);
//            eventsMap.put(curVarName, filteredEvents);
//            // windowTags = updateWindowTags(windowTags);
//            updateWindowTags(windowTags);
////            for(Map.Entry<Long, Byte> entry : windowTags.entrySet()){
////                System.out.println("key: " + entry.getKey() + " value: " + Utils.byteToBinary(entry.getValue()));
////            }
//        }
//        long secondFilterEnd = System.currentTimeMillis();
//        System.out.println("second filter data cost: " + (secondFilterEnd - secondFilterStart) + "ms");
//
//        long mergeStart = System.currentTimeMillis();
//        List<Event> filteredEvents = new ArrayList<>(4096);
//        Set<String> varNames = eventsMap.keySet();
//        for(String varName : varNames){
//            List<Event> curEventList = eventsMap.get(varName);
//            filteredEvents = Utils.mergeSortedEvents(filteredEvents, curEventList);
//            System.out.println("varName: " + varName + " size: " + curEventList.size());
//        }
//        long mergeEnd = System.currentTimeMillis();
//        System.out.println("merge data cost: " + (mergeEnd - mergeStart) + "ms");
//        System.out.println("size of filtered events: " + filteredEvents.size());
//
////        NFA nfa = new NFA();
////        nfa.constructNFA(query);
////
////        long matchStart = System.currentTimeMillis();
////        for(Event event : filteredEvents){
////            nfa.consume(event, query.getStrategy());
////        }
////        long matchEnd = System.currentTimeMillis();
////        System.out.println("matching cost: " + (matchEnd - matchStart) + "ms");
////
////        nfa.getFullMatchEventsStatistic();
////        List<String> matches  = nfa.getAllMatchedResults();
////        System.out.println("result size: " + matches.size());
//    }
//}
//
////        PrintStream originOut = System.out;
////        PrintStream printStream = new PrintStream(prefixPath + "java" + sep + "output" + sep + "our_crimes_events.txt");
////        System.setOut(printStream);
////        for(Event event : mergedEvents){
////            System.out.println(event);
////        }
////        System.setOut(originOut);

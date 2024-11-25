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
//public class TestReplayIntervalFiltering {
//
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
//    public static Map<String, List<Event>> checkVariableIndependentConstraint(Map<String, String> varTypeMap,
//                                                                              HashMap<String, List<IndependentPredicate>> varIpList,
//                                                                                                                                                    List<Event> events){
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
//                            if (!satisfy) {
//                                break;
//                            }
//                        }
//                    }
//                    // if it is satisfied, then we store it
//                    if (satisfy) {
//                        filteredEvents.get(curVarName).add(event);
//                        // write file... System.out.println(event);
//                    }
//                }
//            }
//        }
//        return filteredEvents;
//    }
//
//    public static ReplayIntervals generateReplayIntervals(List<Event> events, long queryWindow, int headTailCase){
//        ReplayIntervals replayIntervals = new ReplayIntervals(events.size());
//
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
//        for(Event event : events){
//            long ts= event.getTimestamp();
//            replayIntervals.insert(ts + leftOffset, ts + rightOffset);
//        }
//
//        return replayIntervals;
//    }
//
//    public static List<Event> intervalFiltering(List<Event> events, ReplayIntervals replayIntervals, boolean eventInOrder){
//        if(!eventInOrder){
//            events.sort(Comparator.comparing(Event::getTimestamp));
//        }
//        return replayIntervals.filtering(events, true);
//    }
//
//    public static void main(String[] args) throws Exception{
//
//
//        String sep = File.separator;
//        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
//
//        // v1.beat >= 1900 AND v1.beat <= 2000 AND v1.District = v2.District AND v1.District = v3.District
//        // v1.beat <= 3000
//        String queryStr = "PATTERN SEQ(ROBBERY v1, BATTERY v2, MOTOR_VEHICLE_THEFT v3)\n" +
//                "USING SKIP_TILL_ANY_MATCH\n" +
//                "WHERE v1.District = v2.District AND v1.District = v3.District\n" +
//                "WITHIN 1800 units";   // here 1800 units = 30 minutes
//        PatternQuery query = QueryParse.parseQueryString(queryStr);
//        long queryWindow = query.getQueryWindow();
//
//        Map<String, String> varTypeMap = query.getVarTypeMap();
//        query.print();
//
//        // load data: crimes_test
//        String dataFilePath = prefixPath + "dataset" + sep + "crimes.csv";
//
//        long loadStart = System.currentTimeMillis();
//        List<Event> events = loadData(dataFilePath);
//        long loadEnd = System.currentTimeMillis();
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
//            System.out.println("varName: " + varName + " size: " + size);
//        }
//        Arrays.sort(statistics);
//
//        // generate replay intervals
//        ReplayIntervals replayIntervals = generateReplayIntervals(eventsMap.get(statistics[0].varName), queryWindow, statistics[0].headTailCase);
//
//        // filtering
//        for(int i = 1; i < statistics.length; ++i) {
//            Statistic statistic = statistics[i];
//            String curVarName = statistic.varName;
//            List<Event> curVarEvents = eventsMap.get(curVarName);
//            List<Event> filteredEvents = intervalFiltering(curVarEvents, replayIntervals, true);
//            eventsMap.put(curVarName, filteredEvents);
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
////        Runtime runtime = Runtime.getRuntime();
////        long maxMemory = runtime.maxMemory() / 1024 / 1024/ 1024;
////        System.out.println("xmx: " + maxMemory + "GB");
////        PrintStream originOut = System.out;
////        PrintStream printStream = new PrintStream(prefixPath + "java" + sep + "output" + sep + "replay_intervals_crimes_events.txt");
////        System.setOut(printStream);
////        for(Event event : mergedEvents){
////            System.out.println(event);
////        }
////        System.setOut(originOut);

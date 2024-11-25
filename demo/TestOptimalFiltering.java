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
//public class TestOptimalFiltering {
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
//                                                                 HashMap<String, List<IndependentPredicate>> varIpList,
//                                                                 List<Event> events){
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
//    public static void main(String[] args) throws FileNotFoundException {
//        String sep = File.separator;
//        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
//
//        // v1.beat >= 1900 AND v1.beat <= 2000 AND v1.District = v2.District AND v1.District = v3.District
//        // v1.beat >= 1900 AND v1.beat <= 2000 AND V1.DISTRICT = V3.DISTRICT AND V2.DISTRICT = V3.DISTRICT
//        // v1.beat <= 3000
//        String queryStr = "PATTERN SEQ(ROBBERY v1, BATTERY v2, MOTOR_VEHICLE_THEFT v3)\n" +
//                "USING SKIP_TILL_ANY_MATCH\n" +
//                "WHERE v1.beat >= 1900 AND v1.beat <= 2000 AND V1.DISTRICT = V3.DISTRICT AND V2.DISTRICT = V3.DISTRICT\n" +
//                "WITHIN 1800 units";   // here 1800 units = 30 minutes
//        PatternQuery query = QueryParse.parseQueryString(queryStr);
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
//        long mergeStart = System.currentTimeMillis();
//        List<Event> filteredEvents = new ArrayList<>(4096);
//        Set<String> varNames = eventsMap.keySet();
//        for(String varName : varNames){
//            List<Event> curEventList = eventsMap.get(varName);
//            filteredEvents = Utils.mergeSortedEvents(filteredEvents, curEventList);
//        }
//        long mergeEnd = System.currentTimeMillis();
//        System.out.println("merge data cost: " + (mergeEnd - mergeStart) + "ms");
//
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

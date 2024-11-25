package rpc;

import filter.LockFreeBloomFilter;
import filter.ShrinkFilterUltra;
import filter.UpdatedMarkers;
import org.apache.thrift.TException;
import query.EqualDependentPredicate;
import rpc.iface.FilterUltraRPC;
import store.EventCache;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.*;

public class FilterUltraRPCImpl implements FilterUltraRPC.Iface{
    private EventCache cache;
    private List<String> hasFilteredVarNames;
    private ShrinkFilterUltra shrinkFilterUltra;

    @Override
    public Map<String, Integer> initial(String tableName, Map<String, List<String>> ipStrMap) {
        long startTime = System.currentTimeMillis();

        FullScan fullscan = new FullScan(tableName);
        cache = fullscan.scanBasedVarName(ipStrMap);
        hasFilteredVarNames = new ArrayList<>(8);
        shrinkFilterUltra = null;
        Map<String, Integer> res = cache.getCardinality();

        long endTime = System.currentTimeMillis();
        System.out.println("scan cost: " + (endTime - startTime) + "ms");

        return res;
    }

    @Override
    public ByteBuffer getReplayIntervals(String varName, long window, int headTailMarker) {
        long startTime = System.currentTimeMillis();

        ByteBuffer intervalBuffer = cache.generateReplayIntervals(varName, window, headTailMarker);
        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("generate interval cost: " + (endTime - startTime) + "ms");

        return intervalBuffer;
    }

    @Override
    public Map<String, ByteBuffer> getBF4EQJoin(String varName, long window, Map<String, List<String>> dpStrMap, Map<String, Integer> keyNumMap, ByteBuffer buff) {
        long buildStartTime = System.currentTimeMillis();
        // you can see details from FilterRPCImpl.java
        if(hasFilteredVarNames.size() == 1){
            shrinkFilterUltra = ShrinkFilterUltra.deserialize(buff);
        }else{
            UpdatedMarkers markers = UpdatedMarkers.deserialize(buff);
            shrinkFilterUltra.rebuild(markers);
        }
        int visitedVarNum = hasFilteredVarNames.size();
        for(int i = 0; i < visitedVarNum - 1; i++){
            cache.simpleFilter(hasFilteredVarNames.get(i), window, shrinkFilterUltra);
        }

        int dpMapSize = dpStrMap.size();

        Map<String, ByteBuffer> bfMap = new HashMap<>(dpMapSize << 1);
        for(String preVarName : dpStrMap.keySet()){
            List<String> dpStrList = dpStrMap.get(preVarName);
            List<EqualDependentPredicate> dpList = new ArrayList<>(dpStrList.size());
            for(String dpStr : dpStrList){
                dpList.add(new EqualDependentPredicate(dpStr));
            }
            ByteBuffer buffer = cache.generateBloomFilter(preVarName, keyNumMap.get(preVarName), window, dpList);

            System.out.println("generate bf size: " + buffer.capacity() + " bytes");

            bfMap.put(preVarName, buffer);
        }

        long buildEndTime = System.currentTimeMillis();
        System.out.println("generate bf cost: " + (buildEndTime - buildStartTime) + "ms");

        return bfMap;
    }

    @Override
    public ByteBuffer getHashTable4NEQJoin(String varName, long window, List<String> neqDPStr, ByteBuffer buff){
        // waiting write code
        return null;
    }

    @Override
    public ByteBuffer windowFilter(String varName, long window, int headTailMarker, ByteBuffer buff) {
        // please note that we return UpdatedMarkers
        long startTime = System.currentTimeMillis();
        // you can see details from FilterRPCImpl.java
        if(hasFilteredVarNames.size() == 1){
            shrinkFilterUltra = ShrinkFilterUltra.deserialize(buff);
        }else{
            UpdatedMarkers markers = UpdatedMarkers.deserialize(buff);
            shrinkFilterUltra.rebuild(markers);
        }
        // 2024-11-19, we have to update markers + filtered event number
        ByteBuffer updatedMarkers = cache.updatePointers(varName, window, headTailMarker, shrinkFilterUltra);
        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");

        return updatedMarkers;
    }

    @Override
    public ByteBuffer eqJoinFilter(String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> dpStrMap, Map<String, ByteBuffer> bfBufferMap) throws TException {
        // please note that we return UpdatedMarkers
        long startTime = System.currentTimeMillis();

        int previousVarNum = previousOrNext.size();
        Map<String, LockFreeBloomFilter> bfMap = new HashMap<>(previousVarNum << 1);
        Map<String, List<EqualDependentPredicate>> dpMap = new HashMap<>(previousVarNum << 1);
        for(String previousVariableName : bfBufferMap.keySet()){
            ByteBuffer buffer = bfBufferMap.get(previousVariableName);
            LockFreeBloomFilter bf = LockFreeBloomFilter.deserialize(buffer);
            bfMap.put(previousVariableName, bf);

            List<EqualDependentPredicate> dps = new ArrayList<>();
            List<String> dpStrList = dpStrMap.get(previousVariableName);
            for(String dpStr : dpStrList){
                dps.add(new EqualDependentPredicate(dpStr));
            }
            dpMap.put(previousVariableName, dps);
        }
        ByteBuffer updatedMarkers = cache.updatePointers(varName, window, headTailMarker, shrinkFilterUltra, previousOrNext, bfMap, dpMap);
        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");

        return updatedMarkers;
    }

    @Override
    public ByteBuffer neqJoinFilter(String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> dpStrMap, Map<String, MinMaxPair> valueRange) throws TException {
        // 这个函数目前用来测试用
        // max -len 100M
        // 190_000_000
        int size = 16 * 1024 * 1024;
        long start = System.currentTimeMillis();
        byte ch = 0;
        byte[] content = new byte[size];
//        for(int i = 0; i < size; ++i){
//            content[i] = ch++;
//        }
        ByteBuffer buffer = ByteBuffer.wrap(content);
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        return buffer;
    }

    @Override
    public ByteBuffer getAllFilteredEvents(long window, ByteBuffer updatedMarkers){
        long startTime = System.currentTimeMillis();

        UpdatedMarkers markers = UpdatedMarkers.deserialize(updatedMarkers);
        shrinkFilterUltra.rebuild(markers);
        ByteBuffer buffer = cache.getRecords(window, shrinkFilterUltra);

        long endTime = System.currentTimeMillis();
        System.out.println("write result cost: " + (endTime - startTime) + "ms");

        // clear
        cache = null;
        hasFilteredVarNames = null;
        shrinkFilterUltra = null;

        return buffer;
    }
}

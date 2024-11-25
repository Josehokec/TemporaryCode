package rpc;

import filter.LockFreeBloomFilter;
import filter.ShrinkFilter;
import org.apache.thrift.TException;
import query.EqualDependentPredicate;
import rpc.iface.FilterRPC;
import store.EventCache;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * this filter contains three fields: fingerprint, interval_marker, hit_marker
 * the reason why keep ShrinkFilter in this class because we need to update pointers
 */
public class FilterRPCImpl implements FilterRPC.Iface{
    public static EventCache cache;
    public static List<String> hasFilteredVarNames;
    public static ShrinkFilter shrinkFilter;

    @Override
    public Map<String, Integer> initial(String tableName, Map<String, List<String>> ipStrMap){
        System.out.println("new query arrives.....");
        long startTime = System.currentTimeMillis();

        FullScan fullscan = new FullScan(tableName);
        cache = fullscan.scanBasedVarName(ipStrMap);
        hasFilteredVarNames = new ArrayList<>(8);
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
    public Map<String, ByteBuffer> getBF4EQJoin(String varName, long window, Map<String, List<String>> dpStrMap, Map<String, Integer> keyNumMap, ByteBuffer sfBuffer) {
        // get bloom filter for join operation,
        // e.g., t1.a = t2.a and t1.b = t2.b
        // request sequence: a -> b
        // input: varName is b, dpStrMap is {a, [t1.a = t2.a, t1.b = t2.b]}

        long buildStartTime = System.currentTimeMillis();

        // before building bloom filter, we will remove events
        shrinkFilter = ShrinkFilter.deserialize(sfBuffer);
        int visitedVarNum = hasFilteredVarNames.size();
        for(int i = 0; i < visitedVarNum - 1; i++){
            cache.simpleFilter(hasFilteredVarNames.get(i), window, shrinkFilter);
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
    public Map<String, MinMaxPair> getHashTable4NEQJoin(String varName, long window, List<String> neqDPStr, ByteBuffer sfBuffer) {
        // waiting write code
        return Collections.emptyMap();
    }

    @Override
    public ByteBuffer windowFilter(String varName, long window, int headTailMarker, ByteBuffer sfBuffer) {
        long startTime = System.currentTimeMillis();

        ShrinkFilter filter = ShrinkFilter.deserialize(sfBuffer);
        ByteBuffer updatedShrinkFilter = cache.updatePointers(varName, window, headTailMarker, filter);
        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");

        return updatedShrinkFilter;
    }

    @Override
    public ByteBuffer eqJoinFilter(String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> dpStrMap, Map<String, ByteBuffer> bfBufferMap) {
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
        ByteBuffer updatedShrinkFilter = cache.updatePointers(varName, window, headTailMarker, shrinkFilter, previousOrNext, bfMap, dpMap);
        hasFilteredVarNames.add(varName);
        shrinkFilter = null;                // clear shrink filter

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");

        return updatedShrinkFilter;
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
    public ByteBuffer getAllFilteredEvents(long window, ByteBuffer sfBuffer) throws TException {
        long startTime = System.currentTimeMillis();

        ShrinkFilter filter = ShrinkFilter.deserialize(sfBuffer);
        ByteBuffer buffer = cache.getRecords(window, filter);

        long endTime = System.currentTimeMillis();
        System.out.println("write result cost: " + (endTime - startTime) + "ms");

        return buffer;
    }
}

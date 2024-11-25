package rpc;

import org.apache.thrift.TException;
import store.EventCache;
import store.FullScan;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class IntervalSetServiceImpl implements IntervalSetService.Iface{
    private EventCache cache;

    @Override
    public Map<String, Integer> initial(String tableName, Map<String, List<String>> independentPredicateMap) throws TException {
        long startTime = System.currentTimeMillis();
        FullScan fullscan = new FullScan(tableName);
        cache = fullscan.scanBasedVarName(independentPredicateMap);
        Map<String, Integer> res = cache.getCardinality();
        long endTime = System.currentTimeMillis();
        System.out.println("scan cost: " + (endTime - startTime) + "ms");
        return res;
    }

    @Override
    public ByteBuffer generateIntervals(String variableName, long window, int headTailMarker) throws TException {
        long startTime = System.currentTimeMillis();
        ByteBuffer intervalBuffer = cache.generateReplayIntervals(variableName, window, headTailMarker);
        long endTime = System.currentTimeMillis();
        System.out.println("generate interval cost: " + (endTime - startTime) + "ms");
        return intervalBuffer;
    }

    @Override
    public ByteBuffer filterBasedWindow(String variableName, long window, int headTailMarker, ByteBuffer intervalSet) throws TException {
        long startTime = System.currentTimeMillis();
        ReplayIntervals intervals = ReplayIntervals.deserialize(intervalSet);
        ByteBuffer intervalBuffer = cache.updatePointers(variableName, window, headTailMarker, intervals);
        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + variableName + " filter cost: " + (endTime - startTime) + "ms");
        return intervalBuffer;
    }

    @Override
    public ByteBuffer getAllFilteredEvents(ByteBuffer intervalSet) throws TException {
        long startTime = System.currentTimeMillis();
        ReplayIntervals intervals = ReplayIntervals.deserialize(intervalSet);
        ByteBuffer buffer = cache.getRecords(intervals);
        long endTime = System.currentTimeMillis();
        System.out.println("write result cost: " + (endTime - startTime) + "ms");
        return buffer;
    }
}

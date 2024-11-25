package store;

import filter.LockFreeBloomFilter;
import filter.ShrinkFilter;
import filter.ShrinkFilterUltra;
import filter.UpdatedMarkers;
//import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import query.DependentPredicate;
import query.EqualDependentPredicate;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;


/**
 * cache the events that maybe involved in matching
 * then we can avoid to access multiple disk
 * note that we assume that all related events can be stored in caches
 * in EventCache class, we define many function for different methods
 */
public class EventCache {
    private final EventSchema schema;
    //private int cacheRecordNum;
    private final int EQUAL_DIVISION = 20;

    // below two variables may occupy large space, we plan to compress these data
    // if the number of records is big, maybe occur out of memory
    List<byte[]> records;
    // if out-of-memory we need to compress pointers
    Map<String, List<Integer>> varPointers;         // here we can save space

    public EventCache(EventSchema schema, List<byte[]> records, Map<String, List<Integer>> varPointers) {
        this.schema = schema;
        //this.cacheRecordNum = cacheRecordNum;
        this.records = records;
        this.varPointers = varPointers;
    }

    // this function is used for updating cache
    public List<Integer> getMergedPointers(){
        List<Integer> mergedPointers = new ArrayList<>(8);
        for(List<Integer> pointers : varPointers.values()){
            mergedPointers = merge(mergedPointers, pointers);
        }
        return mergedPointers;
    }

    // when replay/time intervals shrink, many events cannot involve in matching
    // in other words, the size of varPointers become small
    // to reduce memory overhead, we need to delete these events
    public double flushCache(){
        List<Integer> mergedPointers = getMergedPointers();

        // here we use deep copy rather than shallow copy, which leads to long update cost
        int size = mergedPointers.size();
        int dataLen = schema.getFixedRecordLen();

        byte[][] data = new byte[size][dataLen];
        List<byte[]> updatedRecords = new ArrayList<>(size);
        // old pointers -> new pointers
        Map<Integer, Integer> pointerMap = new HashMap<>(size << 1);
        int count = 0;
        for(Integer pointer : mergedPointers){
            byte[] record = records.get(pointer);
            // here we use
            byte[] copiedRecord = data[count];
            System.arraycopy(record, 0, copiedRecord, 0, dataLen);
            updatedRecords.add(copiedRecord);
            pointerMap.put(pointer, count);
            count++;
        }
        int oldSize = records.size();
        records = updatedRecords;

        for(String key : varPointers.keySet()){
            List<Integer> oldPointers = varPointers.get(key);
            int pointerNum = oldPointers.size();
            List<Integer> newPointers = new ArrayList<>(pointerNum);
            for(Integer pointer : oldPointers){
                newPointers.add(pointerMap.get(pointer));
            }
            varPointers.put(key, newPointers);
        }

        return (size + 0.0) / oldSize;
    }

    /**
     * given a variable name, obtain all events
     * this function is only used for debugging
     * @param varName   variable name
     * @return          events that satisfy the variable's constraints
     */
    public List<byte[]> getVarRecords(String varName){
        List<Integer> pointers = varPointers.get(varName);
        List<byte[]> ans = new ArrayList<>(pointers.size());
        for(Integer pointer : pointers){
            ans.add(records.get(pointer));
        }
        return ans;
    }

    // debug
    public void display(){
        for(String key : varPointers.keySet()){
            List<Integer> pointers = varPointers.get(key);
            System.out.println("key: " + key + ", pointers size: " + pointers.size());
        }
    }

    public Map<String, Integer> getCardinality(){
        Map<String, Integer> cardinalityMap = new HashMap<>(varPointers.size() << 1);
        for(String varName : varPointers.keySet()){
            cardinalityMap.put(varName, varPointers.get(varName).size());
        }
        return cardinalityMap;
    }

    // we choose the variable with the lowest selectivity to generate replay interval
    public ByteBuffer generateReplayIntervals(String varName, long window, int headTailMarker){
        // 0: head variable, 1: tail variable, 2: middle variable
        List<Integer> pointers = varPointers.get(varName);
        ReplayIntervals intervals = new ReplayIntervals(pointers.size());

        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        for(Integer pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            intervals.insert(ts + leftOffset, ts + rightOffset);
        }

        return intervals.serialize();
    }

    public ByteBuffer generateIntervalBitmap(String varName, long window, int headTailMarker) {
        // 0: head variable, 1: tail variable, 2: middle variable
        List<Integer> pointers = varPointers.get(varName);
        //RoaringBitmap bitmap = new RoaringBitmap();
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        // equal division

        for(Integer pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            long convertedStart = (ts + leftOffset) * EQUAL_DIVISION / window;
            long convertedEnd = (ts + rightOffset) * EQUAL_DIVISION / window;
            //bitmap.add(convertedStart, convertedEnd + 1);
            bitmap.addRange(convertedStart, convertedEnd + 1);
        }

        long size = bitmap.serializedSizeInBytes();
        ByteBuffer buffer = ByteBuffer.allocate((int) size);

        try{
            bitmap.serialize(buffer);
        }catch (Exception e){
            e.printStackTrace();
        }
        //debug
        System.out.println("varName: " + varName + " 's bitmap size: " + size);
        buffer.flip();
        return buffer;
    }

    // 例子：a.A1 = b.A1 AND a.A2 = b.A2
    // 这里假设请求顺序是a -> b,则为a先构建bloom filter
    // 这个地方比较难写
    public ByteBuffer generateBloomFilter(String varName, int eventNum, long window, List<EqualDependentPredicate> dps){

        double DEFAULT_FPR = 0.001;
        LockFreeBloomFilter bf = new LockFreeBloomFilter(DEFAULT_FPR, eventNum);

        List<Integer> pointers = varPointers.get(varName);
        System.out.println("bf-varName: " + varName + " estimate eventNum: " + eventNum + " real num: " + pointers.size());

        for(Integer pointer : pointers){
            byte[] record = records.get(pointer);
            long timestamp = schema.getTimestamp(record);
            StringBuilder stringBuilder = new StringBuilder(16);
            for(EqualDependentPredicate dp : dps){
                String attrName = dp.getAttributeName();
                DataType dataType = schema.getDataType(attrName);
                Object obj = dp.getOneSideValue(varName, schema.getColumnValue(attrName, record), dataType);
                stringBuilder.append(obj.toString());
            }
            String key = stringBuilder.toString();
            // here window id is the seed of hash function
            bf.insertWithWindow(key, timestamp / window);
        }
        return bf.serialize();
    }

    public void simpleFilter(String varName, long window, ShrinkFilter shrinkFilter){
        List<Integer> pointers = varPointers.get(varName);

        System.out.println("begin simple filter, variable name: " +varName + " num: " + pointers.size());

        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(shrinkFilter.query(ts, window)){
                updatedPointers.add(pointer);
            }
        }
        System.out.println("end simple filter, variable name: " + varName + " num: " + pointers.size());
        varPointers.put(varName, updatedPointers);
    }

    public void simpleFilter(String varName, long window, ShrinkFilterUltra shrinkFilterUltra){
        List<Integer> pointers = varPointers.get(varName);
        //System.out.println("begin simple filter, variable name: " +varName + " num: " + pointers.size());
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(shrinkFilterUltra.query(ts, window)){
                updatedPointers.add(pointer);
            }
        }
        System.out.println("end simple filter, variable name: " + varName + " num: " + pointers.size());
        varPointers.put(varName, updatedPointers);
    }

    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ShrinkFilter shrinkFilter){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        // code optimization: delay update range, because if many event has same timestamp update has a high cost
        ReplayIntervals updateIntervals = new ReplayIntervals(256);
        long previousNonExistWindowId = -1;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            long curWindowId = ts/ window;

            // 这个地方只对事件有序有作用哈
            // 如果大部分的事件不满足的话，那么二次查询是有性能提升的
            if(previousNonExistWindowId != curWindowId){
                if(shrinkFilter.queryWindowId(curWindowId)){
                    if(shrinkFilter.query(ts, window)){
                        updateIntervals.insert(ts + leftOffset, ts + rightOffset);
                        updatedPointers.add(pointer);
                    }
                }else{
                    previousNonExistWindowId = curWindowId;
                }
            }

            // old version: out of order
            //if(shrinkFilter.query(ts, window)){
            //    shrinkFilter.updateRange(ts + leftOffset, ts + rightOffset, window);
            //    updatedPointers.add(pointer);
            //}
        }

        List<ReplayIntervals.TimeInterval> intervals = updateIntervals.getIntervals();
        for(ReplayIntervals.TimeInterval interval : intervals){
            shrinkFilter.updateRange(interval.getStartTime(), interval.getEndTime(), window);
        }

        varPointers.put(varName, updatedPointers);
        return shrinkFilter.serialize();
    }

    // 为了让join评估的更准确，这个地方我们不得不再返回在间隙内的事件数量？
    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ShrinkFilterUltra shrinkFilterUltra){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);
        // suppose we can filter quarter of events
        List<Integer> updatedPointers = new ArrayList<>(pointers.size() >> 2);
        ReplayIntervals intervals = new ReplayIntervals(2048);
        UpdatedMarkers updatedMarkers = new UpdatedMarkers(shrinkFilterUltra.getBucketNum());

        // when events arrival in ordered, we can speedup filtering
        int filteredEventNum = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(shrinkFilterUltra.query(ts, window)){
                filteredEventNum++;
                updatedPointers.add(pointer);
                intervals.insert(ts + leftOffset, ts + rightOffset);
            }
        }

        // if events are ordered, we can use replay intervals to accelerate update
        // if events are out-of-ordered, we still have a good update latency
        List<ReplayIntervals.TimeInterval> timeIntervals = intervals.getIntervals();
        for(ReplayIntervals.TimeInterval timeInterval : timeIntervals){
            long start = timeInterval.getStartTime();
            long end = timeInterval.getEndTime();
            long[][] windowIdAndMarkers = ShrinkFilterUltra.getWindowIdAndMarkers(start, end, window);
            for (long[] windowIdAndMarker : windowIdAndMarkers) {
                long wid = windowIdAndMarker[0];
                long marker = windowIdAndMarker[1];
                int[] pair = shrinkFilterUltra.queryWindowId(wid);
                if (pair[0] != -1) {
                    updatedMarkers.update(pair[0], pair[1], marker);
                }
            }
        }
        varPointers.put(varName, updatedPointers);
        return updatedMarkers.serialize(filteredEventNum);
    }

    /**
     * this function has a litter complex, for example, we need to filter c (a and b have been filtered)
     * the dependent predicates are: a.A1 = c.A1 AND b.A1 = c.A1 AND b.A2 = c.A2 + 4
     * then we will use two bloom filter, first bloom filter is constructed on variable a and key is A1
     * second bloom filter is constructed on variable b and key is A1|A2
     * @param varName                   current variable that needs to filter
     * @param window                    query window
     * @param headTailMarker            is head variable or rear variable
     * @param shrinkFilter              shrink filter
     * @param previousOrNext            previous then window ID - 1, next then window ID + 1 (more complex pattern need adjust this value)
     * @param bfMap                     bloom filter list
     * @param dependentPredicateMap     dependent predicate map
     * @return                          shrink filter
     */
    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ShrinkFilter shrinkFilter, Map<String, Boolean> previousOrNext,
                                     Map<String, LockFreeBloomFilter> bfMap, Map<String, List<EqualDependentPredicate>> dependentPredicateMap){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);

        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(shrinkFilter.query(ts, window)){
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){
                    StringBuilder stringBuilder = new StringBuilder(16);
                    LockFreeBloomFilter lbf = bfMap.get(preVarName);
                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);
                    // check dependent predicate
                    for(DependentPredicate dp : dps){
                        String attrName = dp.getAttributeName();
                        DataType dataType = schema.getDataType(attrName);
                        Object obj = dp.getOneSideValue(varName, schema.getColumnValue(attrName, record), dataType);
                        stringBuilder.append(obj.toString());
                    }
                    String key = stringBuilder.toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){
                        if(!lbf.containsWithWindow(key, ts/ window) && !lbf.containsWithWindow(key, ts/ window - 1)){
                            satisfied = false;
                            break;
                        }
                    }else{
                        if(!lbf.containsWithWindow(key, ts/ window) && !lbf.containsWithWindow(key, ts/ window + 1)){
                            satisfied = false;
                            break;
                        }
                    }
                }

                if(satisfied){
                    shrinkFilter.updateRange(ts + leftOffset, ts + rightOffset, window);
                    updatedPointers.add(pointer);
                }
            }
        }
        varPointers.put(varName, updatedPointers);
        return shrinkFilter.serialize();
    }

    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ShrinkFilterUltra shrinkFilterUltra, Map<String, Boolean> previousOrNext,
                                     Map<String, LockFreeBloomFilter> bfMap, Map<String, List<EqualDependentPredicate>> dependentPredicateMap){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;
        UpdatedMarkers updatedMarkers = new UpdatedMarkers(shrinkFilterUltra.getBucketNum());
        List<Integer> pointers = varPointers.get(varName);

        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        // use this to accelerate update
        ReplayIntervals intervals = new ReplayIntervals(1024);
        int filteredEventNum = 0;

        int cnt = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);

            if(shrinkFilterUltra.query(ts, window)){
                cnt++;
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){
                    StringBuilder stringBuilder = new StringBuilder(16);
                    LockFreeBloomFilter lbf = bfMap.get(preVarName);
                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);
                    // check dependent predicate
                    for(DependentPredicate dp : dps){
                        String attrName = dp.getAttributeName();
                        DataType dataType = schema.getDataType(attrName);
                        Object obj = dp.getOneSideValue(varName, schema.getColumnValue(attrName, record), dataType);
                        stringBuilder.append(obj.toString());
                    }
                    String key = stringBuilder.toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){
                        if(!lbf.containsWithWindow(key, ts / window) && !lbf.containsWithWindow(key, ts / window + 1)){
                            satisfied = false;
                            break;
                        }
                    }else{
                        if(!lbf.containsWithWindow(key, ts/ window) && !lbf.containsWithWindow(key, ts/ window - 1)){
                            satisfied = false;
                            break;
                        }
                    }
                }
                // if satisfy window condition and join condition
                if(satisfied){
                    filteredEventNum++;
                    intervals.insert(ts + leftOffset, ts + rightOffset);
                    updatedPointers.add(pointer);
                }
            }
        }

        System.out.println("varName: " + varName + " original number of events:" + cnt);
        System.out.println("filteredEventNum: " + filteredEventNum);

        // finally, update
        List<ReplayIntervals.TimeInterval> timeIntervals = intervals.getIntervals();
        for(ReplayIntervals.TimeInterval timeInterval : timeIntervals){
            long start = timeInterval.getStartTime();
            long end = timeInterval.getEndTime();
            long[][] windowIdAndMarkers = ShrinkFilterUltra.getWindowIdAndMarkers(start, end, window);
            for (long[] windowIdAndMarker : windowIdAndMarkers) {
                long wid = windowIdAndMarker[0];
                long marker = windowIdAndMarker[1];
                int[] pair = shrinkFilterUltra.queryWindowId(wid);
                if (pair[0] != -1) {
                    updatedMarkers.update(pair[0], pair[1], marker);
                }
            }
        }

        varPointers.put(varName, updatedPointers);
        return updatedMarkers.serialize(filteredEventNum);
    }

    // return bitmap
    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, Roaring64Bitmap bitmap) {
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        Roaring64Bitmap updatedBitmap = new Roaring64Bitmap();

        System.out.println("before filtering, key number: " + pointers.size());

        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            int convertedKey = (int) (ts * EQUAL_DIVISION / window);
            if(bitmap.contains(convertedKey)){
                long convertedStart = (ts + leftOffset) * EQUAL_DIVISION / window;
                long convertedEnd = (ts + rightOffset) * EQUAL_DIVISION / window;
                updatedBitmap.add(convertedStart, convertedEnd + 1);
                updatedPointers.add(pointer);
            }
        }

        System.out.println("after filtering, key number:" + updatedPointers.size());

        varPointers.put(varName, updatedPointers);
        updatedBitmap.and(bitmap);

        long size = updatedBitmap.serializedSizeInBytes();
        ByteBuffer buffer = ByteBuffer.allocate((int) size);

        try{
            updatedBitmap.serialize(buffer);
        }catch (Exception e){
            e.printStackTrace();
        }
        //debug
        System.out.println("varName: " + varName + " 's bitmap size: " + size);
        buffer.flip();
        return buffer;
    }

    // return interval set
    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ReplayIntervals intervals){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        intervals.sortAndReconstruct();
        ReplayIntervals newIntervals = new ReplayIntervals();

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        System.out.println("before filtering, key number: " + pointers.size());
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            // if events are ordered, we can use prefetchContains function to obtain better performance
            if(intervals.contains(ts)){
                newIntervals.insert(ts + leftOffset, ts + rightOffset);
                updatedPointers.add(pointer);
            }
        }
        System.out.println("after filtering, key number:" + updatedPointers.size());
        varPointers.put(varName, updatedPointers);
        intervals.intersect(newIntervals);
        return intervals.serialize();
    }

    // send all records that maybe involve in matching to computer node
    public ByteBuffer getRecords(long window, ShrinkFilter shrinkFilter){
        List<Integer> mergedPointers = getMergedPointers();

        int dataLen = schema.getFixedRecordLen();
        int size = mergedPointers.size();
        ByteBuffer buffer = ByteBuffer.allocate(dataLen * size);

        // if events are ordered, we need to optimization:
        // query get range?
        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(shrinkFilter.query(ts, window)){
                buffer.put(record);

            }
        }
        buffer.flip();
        ByteBuffer truncatedBuffer = ByteBuffer.allocate(buffer.remaining());
        truncatedBuffer.put(buffer);
        truncatedBuffer.flip();

        return truncatedBuffer;
    }

    public ByteBuffer getRecords(long window, Roaring64Bitmap bitmap){
        List<Integer> mergedPointers = getMergedPointers();

        int dataLen = schema.getFixedRecordLen();
        int size = mergedPointers.size();
        ByteBuffer buffer = ByteBuffer.allocate(dataLen * size);

        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            int convertedKey = (int) (ts * EQUAL_DIVISION / window);
            if(bitmap.contains(convertedKey)){
                buffer.put(record);
            }
        }
        buffer.flip();

        ByteBuffer truncatedBuffer = ByteBuffer.allocate(buffer.remaining());
        truncatedBuffer.put(buffer);
        truncatedBuffer.flip();

        return truncatedBuffer;
    }

    public ByteBuffer getRecords(ReplayIntervals intervals){
        List<Integer> mergedPointers = getMergedPointers();

        int dataLen = schema.getFixedRecordLen();
        int size = mergedPointers.size();
        ByteBuffer buffer = ByteBuffer.allocate(dataLen * size);

        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(intervals.contains(ts)){
                buffer.put(record);
            }
        }
        buffer.flip();

        ByteBuffer truncatedBuffer = ByteBuffer.allocate(buffer.remaining());
        truncatedBuffer.put(buffer);
        truncatedBuffer.flip();

        return truncatedBuffer;
    }

    public ByteBuffer getRecords(long window, ShrinkFilterUltra shrinkFilterUltra){
        List<Integer> mergedPointers = getMergedPointers();

        int dataLen = schema.getFixedRecordLen();
        int size = mergedPointers.size();
        ByteBuffer buffer = ByteBuffer.allocate(dataLen * size);

        // if events are ordered, can we optimize query?
        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(shrinkFilterUltra.query(ts, window)){
                buffer.put(record);

            }
        }
        buffer.flip();
        ByteBuffer truncatedBuffer = ByteBuffer.allocate(buffer.remaining());
        truncatedBuffer.put(buffer);
        truncatedBuffer.flip();

        return truncatedBuffer;
    }

    // merge two lists (if there having two same values, we only keep one value)
    private List<Integer> merge(List<Integer> list1, List<Integer> list2){
        if(list1 == null || list1.isEmpty()){
            return list2;
        }

        int size1 = list1.size();
        int size2 = list2.size();
        List<Integer> mergedList = new ArrayList<>(size1 + size2);

        int i = 0, j = 0;
        while(i < size1 && j < size2){
            if(list1.get(i) < list2.get(j)){
                mergedList.add(list1.get(i));
                i++;
            }else if (list1.get(i) > list2.get(j)){
                mergedList.add(list2.get(j));
                j++;
            }else{
                // if there having same two values we only keep one value
                mergedList.add(list1.get(i));
                i++;
                j++;
            }
        }

        while(i < size1){
            mergedList.add(list1.get(i));
            i++;
        }

        while(j < size2){
            mergedList.add(list2.get(j));
            j++;
        }
        return mergedList;
    }



    public void print(){
        for(Map.Entry<String, List<Integer>> entry : varPointers.entrySet()){
            String varName = entry.getKey();
            List<Integer> pointers = entry.getValue();
            System.out.println("varName: " + varName + " -> events:");
            for(Integer pointer : pointers){
                System.out.println("\t" + schema.getRecordStr(records.get(pointer)));
            }

        }
    }

}

/*
//            long curWindowId = ts / window;
            // only can accelerate filtering ordered events
            // we think many events can be filtered
//            if(previousNonExistWindowId != curWindowId){
//                int[] bucketSlotPos = shrinkFilterUltra.queryWindowId(curWindowId);
//                if(bucketSlotPos[0] == -1){
//                    previousNonExistWindowId = curWindowId;
//                }else{
//                    if(shrinkFilterUltra.query(ts, window)){
//                        long startTs = ts + leftOffset;
//                        long endTs = ts + rightOffset;
//                        long[][] windowIdAndMarkers = ShrinkFilterUltra.getWindowIdAndMarkers(startTs, endTs, window);
//                        for (long[] windowIdAndMarker : windowIdAndMarkers) {
//                            long wid = windowIdAndMarker[0];
//                            long marker = windowIdAndMarker[1];
//                            if (wid == curWindowId) {
//                                updatedMarkers.update(bucketSlotPos[0], bucketSlotPos[1], marker);
//                            } else {
//                                int[] pair = shrinkFilterUltra.queryWindowId(wid);
//                                if (pair[0] != -1) {
//                                    updatedMarkers.update(pair[0], pair[1], marker);
//                                }
//                            }
//                        }
//                        updatedPointers.add(pointer);
//                    }
//                }
//            }
 */

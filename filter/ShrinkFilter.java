package filter;

import java.nio.ByteBuffer;

/**
 * 使用说明书：
 * 如果知道key是有序的，那么我们推荐先构造replay intervals，然后根据replay interval去插入到WCF
 * 如果key是无序的，那么直接插入即可，插入的开销理论是是O(1)
 * 假设合并发生在计算段，那么可以在filter里面设立
 * ----------------------------------------------------------------------------------
 * Shrink filter (SF) is used for test a timestamp whether within time interval set.
 * SF is based on Cuckoo filter (please see Cuckoo Filter: Practically Better Than Bloom. In CoNEXT, 2014.)
 * We choose filter rather than hashmap or replay intervals (see utils package) because
 * (1) filter has a lower space overhead and higher load factor,
 * (2) filter is merge-able, and it has O(1) insertion/query/update latency
 * We choose below setting:
 * BUCKET_SIZE = 4, LOAD_FACTOR = 0.955, MAX_KICK_OUT = 550
 */
public class ShrinkFilter {
    // note that original paper MAX_KICK_OUT = 500, due to we swap choose 4 position to kick,
    // to achieve a high load factor, we enlarge MAX_KICK_OUT to 550
    static final int MAX_KICK_OUT = 550;

    // change this value to modify mode, default mode: _8_4_4, _16_8_8, _12_10_10, _8_12_12
    public static SFTableMode SHRINK_FILTER_TABLE_MODE = SFTableMode._12_10_10;
    private static final double LOAD_FACTOR = 0.955;
    // the reason why we do not make this variable to be final
    // because this table can be compressed to reduce its size
    private AbstractSFTable table;
    private int bucketNum;

    private ShrinkFilter(AbstractSFTable table, int bucketNum, SFTableMode mode){
        this.table = table;
        this.bucketNum = bucketNum;
        SHRINK_FILTER_TABLE_MODE = mode;
    }

    public static class Builder {
        private final long maxKeys;
        private AbstractSFTable table;
        //private int bitConfig;
        public Builder(long maxKeys) {
            this.maxKeys = maxKeys;
            table = null;
        }

        public Builder(AbstractSFTable table) {
            maxKeys = -1;
            this.table = table;
        }

        public ShrinkFilter build(){
            int bucketNum;
            if(table != null){
                bucketNum = table.getBucketNum();
            }else{
                bucketNum = CFUtils.getBucketsNeeded(maxKeys, LOAD_FACTOR, 4);
                switch (SHRINK_FILTER_TABLE_MODE){
                    case _8_4_4:
                        table = SFTable8_4_4.create(bucketNum);
                        break;
                    case _8_12_12:
                        table = SFTable8_12_12.create(bucketNum);
                        break;
                    case _12_10_10:
                        table = SFTable12_10_10.create(bucketNum);
                        break;
                    case _16_8_8:
                        table = SFTable16_8_8.create(bucketNum);
                        break;
                    default:
                        System.out.println("we cannot support this mode");
                        table = SFTable8_4_4.create(bucketNum);
                        break;
                }
            }
            return new ShrinkFilter(table, bucketNum, SHRINK_FILTER_TABLE_MODE);
        }
    }

    static class SFVictim {
        private int bucketIndex;
        private int altBucketIndex;
        private long tag;

        //WCFVictim(){ tag = -1; }

        SFVictim(int bucketIndex, int altBucketIndex, long tag){
            this.bucketIndex = bucketIndex;
            this.altBucketIndex = altBucketIndex;
            this.tag = tag;
        }

        int getBucketIndex() {
            return bucketIndex;
        }

        void setBucketIndex(int bucketIndex) {
            this.bucketIndex = bucketIndex;
        }

        int getAltBucketIndex() {
            return altBucketIndex;
        }

        void setAltBucketIndex(int altBucketIndex) {
            this.altBucketIndex = altBucketIndex;
        }

        long getTag() {
            return tag;
        }

        void setTag(long tag) {
            this.tag = tag;
        }
    }

    /**
     * insert a time interval
     * @param startTs - start timestamp
     * @param endTs - end timestamp
     * @param window - query window
     * @return - true if insert successfully
     */
    public boolean insert(long startTs, long endTs, long window){
        // debug
        if(startTs > endTs){
            throw new RuntimeException("start timestamp (" + startTs + ") greater than end timestamp (" + endTs + ")");
        }

        long key = startTs / window;
        long[] intervalMarkers = getIntervalMarkers(startTs, endTs, window);
        int len = intervalMarkers.length;
        for(int i = 0; i < len; i++){
            SFIndexAndFingerprint ip = table.generate(key + i);
            int bucketIndex = ip.index;
            int altBucketIndex  = table.altIndex(bucketIndex, ip.fingerprint);
            long fp = ip.fingerprint;
            long tag;
            switch(SHRINK_FILTER_TABLE_MODE){
                case _8_4_4:
                    tag = (fp << 8) + (intervalMarkers[i] << 4);
                    break;
                case _8_12_12:
                    tag = (fp << 24) + (intervalMarkers[i] << 12);
                    break;
                case _12_10_10:
                    tag = (fp << 20) + (intervalMarkers[i] << 10);
                    break;
                default:
                    //case _16_8_8:
                    tag = (fp << 16) + (intervalMarkers[i] << 8);
                    break;
            }
            if(!put(bucketIndex, altBucketIndex, tag)){
                return false;
            }
        }
        return true;
    }

    // private -> public
    public static int getLeftIntervalMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        int leftIntervalMarker;
        int ptr;
        switch (SHRINK_FILTER_TABLE_MODE){
            case _8_12_12:
                ptr = (int) (distance * 12.0 / window);
                switch (ptr){
                    case 0:
                        // 1111 1111 1111
                        leftIntervalMarker = 0xfff;
                        break;
                    case 1:
                        // 1111 1111 1110
                        leftIntervalMarker = 0xffe;
                        break;
                    case 2:
                        // 1111 1111 1100
                        leftIntervalMarker = 0xffc;
                        break;
                    case 3:
                        // 1111 1111 1000
                        leftIntervalMarker = 0xff8;
                        break;
                    case 4:
                        // 1111 1111 0000
                        leftIntervalMarker = 0xff0;
                        break;
                    case 5:
                        // 1111 1110 0000
                        leftIntervalMarker = 0xfe0;
                        break;
                    case 6:
                        // 1111 1100 0000
                        leftIntervalMarker = 0xfc0;
                        break;
                    case 7:
                        // 1111 1000 0000
                        leftIntervalMarker = 0xf80;
                        break;
                    case 8:
                        // 1111 0000 0000
                        leftIntervalMarker = 0xf00;
                        break;
                    case 9:
                        // 1110 0000 0000
                        leftIntervalMarker = 0xe00;
                        break;
                    case 10:
                        // 1100 0000 0000
                        leftIntervalMarker = 0xc00;
                        break;
                    case 11:
                        // 1000 0000 0000
                        leftIntervalMarker = 0x800;
                        break;
                    default:
                        System.out.println("exception...");
                        leftIntervalMarker = 0x3ff;
                }
                return leftIntervalMarker;
            case _12_10_10:
                ptr = (int) (distance * 10.0 / window);
                switch(ptr){
                    case 0:
                        // 11 1111 1111
                        leftIntervalMarker = 0x3ff;
                        break;
                    case 1:
                        // 11 1111 1110
                        leftIntervalMarker = 0x3fe;
                        break;
                    case 2:
                        // 11 1111 1100
                        leftIntervalMarker = 0x3fc;
                        break;
                    case 3:
                        // 11 1111 1000
                        leftIntervalMarker = 0x3f8;
                        break;
                    case 4:
                        // 11 1111 0000
                        leftIntervalMarker = 0x3f0;
                        break;
                    case 5:
                        // 11 1110 0000
                        leftIntervalMarker = 0x3e0;
                        break;
                    case 6:
                        // 11 1100 0000
                        leftIntervalMarker = 0x3c0;
                        break;
                    case 7:
                        // 11 1000 0000
                        leftIntervalMarker = 0x380;
                        break;
                    case 8:
                        // 11 0000 0000
                        leftIntervalMarker = 0x300;
                        break;
                    case 9:
                        // 10 0000 0000
                        leftIntervalMarker = 0x200;
                        break;
                    default:
                        System.out.println("exception...");
                        leftIntervalMarker = 0x3ff;
                }
                return leftIntervalMarker;
            case _16_8_8:
                // your code is here
                ptr = (int) (distance * 8.0 / window);
                switch(ptr){
                    case 0:
                        // 1111 1111
                        leftIntervalMarker = 0xff;
                        break;
                    case 1:
                        // 1111 1110
                        leftIntervalMarker = 0xfe;
                        break;
                    case 2:
                        // 1111 1100
                        leftIntervalMarker = 0xfc;
                        break;
                    case 3:
                        // 1111 1000
                        leftIntervalMarker = 0xf8;
                        break;
                    case 4:
                        // 1111 0000
                        leftIntervalMarker = 0xf0;
                        break;
                    case 5:
                        // 1110 0000
                        leftIntervalMarker = 0xe0;
                        break;
                    case 6:
                        // 1100 0000
                        leftIntervalMarker = 0xc0;
                        break;
                    case 7:
                        // 1000 0000
                        leftIntervalMarker = 0x80;
                        break;
                    default:
                        System.out.println("exception...");
                        leftIntervalMarker = 0x80;
                }
                return leftIntervalMarker;
            default:
                // _8_4_4: only need two comparison
                if(distance < (window >> 1)){
                    if(distance < (window >> 2)){
                        // less than 1/4 window -> 1111
                        leftIntervalMarker = 0xf;
                    }else{
                        // less than 1/2 window -> 1110
                        leftIntervalMarker = 0xe;
                    }
                }else{
                    if(distance < (window - (window >> 1) + (window >> 2))){
                        // 3/4 window -> 1100
                        leftIntervalMarker = 0xc;
                    }else{
                        // less than 1/4 window -> 1000
                        leftIntervalMarker = 0x8;
                    }
                }
                return leftIntervalMarker;
        }
    }

    public static int getRightIntervalMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int rightIntervalMarker;
        int ptr;
        switch (SHRINK_FILTER_TABLE_MODE) {
            case _8_12_12:
                ptr = (int) (distance * 12.0 / window);
                switch (ptr){
                    case 0:
                        // 0000 0000 0001
                        rightIntervalMarker = 0x001;
                        break;
                    case 1:
                        // 0000 0000 0011
                        rightIntervalMarker = 0x003;
                        break;
                    case 2:
                        // 0000 0000 0111
                        rightIntervalMarker = 0x007;
                        break;
                    case 3:
                        // 0000 0000 1111
                        rightIntervalMarker = 0x00f;
                        break;
                    case 4:
                        // 0000 0001 1111
                        rightIntervalMarker = 0x01f;
                        break;
                    case 5:
                        // 0000 0011 1111
                        rightIntervalMarker = 0x03f;
                        break;
                    case 6:
                        // 0000 0111 1111
                        rightIntervalMarker = 0x07f;
                        break;
                    case 7:
                        // 0000 1111 1111
                        rightIntervalMarker = 0x0ff;
                        break;
                    case 8:
                        // 0001 1111 1111
                        rightIntervalMarker = 0x1ff;
                        break;
                    case 9:
                        // 0011 1111 1111
                        rightIntervalMarker = 0x3ff;
                        break;
                    case 10:
                        // 0111 1111 1111
                        rightIntervalMarker = 0x7ff;
                        break;
                    case 11:
                        // 1111 1111 1111
                        rightIntervalMarker = 0xfff;
                        break;
                    default:
                        System.out.println("exception...");
                        rightIntervalMarker = 0xfff;
                }
                return rightIntervalMarker;
            case _12_10_10:
                ptr = (int) (distance * 10.0 / window);
                switch(ptr){
                    case 0:
                        // 00 0000 0001
                        rightIntervalMarker = 0x001;
                        break;
                    case 1:
                        // 00 0000 0011
                        rightIntervalMarker = 0x003;
                        break;
                    case 2:
                        // 00 0000 0111
                        rightIntervalMarker = 0x007;
                        break;
                    case 3:
                        // 00 0000 1111
                        rightIntervalMarker = 0x00f;
                        break;
                    case 4:
                        // 00 0001 1111
                        rightIntervalMarker = 0x01f;
                        break;
                    case 5:
                        // 00 0011 1111
                        rightIntervalMarker = 0x03f;
                        break;
                    case 6:
                        // 00 0111 1111
                        rightIntervalMarker = 0x07f;
                        break;
                    case 7:
                        // 00 1111 1111
                        rightIntervalMarker = 0x0ff;
                        break;
                    case 8:
                        // 01 1111 1111
                        rightIntervalMarker = 0x1ff;
                        break;
                    case 9:
                        // 11 1111 1111
                        rightIntervalMarker = 0x3ff;
                        break;
                    default:
                        System.out.println("exception...");
                        rightIntervalMarker = 0x3ff;
                }
                return rightIntervalMarker;
            case _16_8_8:
                ptr = (int) (distance * 8.0 / window);
                switch(ptr) {
                    case 0:
                        // 0000 0001
                        rightIntervalMarker = 0x01;
                        break;
                    case 1:
                        // 0000 0011
                        rightIntervalMarker = 0x03;
                        break;
                    case 2:
                        // 0000 0111
                        rightIntervalMarker = 0x07;
                        break;
                    case 3:
                        // 0000 1111
                        rightIntervalMarker = 0x0f;
                        break;
                    case 4:
                        // 0001 1111
                        rightIntervalMarker = 0x1f;
                        break;
                    case 5:
                        // 0011 1111
                        rightIntervalMarker = 0x3f;
                        break;
                    case 6:
                        // 0111 1111
                        rightIntervalMarker = 0x7f;
                        break;
                    case 7:
                        // 1111 1111
                        rightIntervalMarker = 0xff;
                        break;
                    default:
                        System.out.println("exception...");
                        rightIntervalMarker = 0xff;
                }
                return rightIntervalMarker;
            default:
                // case _8_4_4
                if (distance < (window >> 1)) {
                    if (distance < (window >> 2)) {
                        // [0, 1/4) window -> 0001
                        rightIntervalMarker = 0x1;
                    } else {
                        // [1/4, 1/2) window -> 0011
                        rightIntervalMarker = 0x3;
                    }
                } else {
                    if (distance < (window - (window >> 1) + (window >> 2))) {
                        // [1/2, 3/4) window -> 0111
                        rightIntervalMarker = 0x7;
                    } else {
                        // [3/4, 1) window -> 1111
                        rightIntervalMarker = 0xf;
                    }
                }
                return rightIntervalMarker;
        }
    }

    private boolean put(int bucketIndex, int altBucketIndex, long tag){
        //System.out.println("i1: " + bucketIndex + ", i2: " + altBucketIndex + ", tag: " + Long.toHexString(tag));
        if(table.insertToBucket(bucketIndex, tag) || table.insertToBucket(altBucketIndex, tag)){
            return true;
        }
        int kickNum = 0;
        SFVictim victim = new SFVictim(bucketIndex, altBucketIndex, tag);
        while(kickNum < MAX_KICK_OUT){
            if(randomSwap(victim)){
                break;
            }
            kickNum++;
        }

        return kickNum != MAX_KICK_OUT;
    }

    private boolean randomSwap(SFVictim victim){
        // always choose alt bucket index
        int bucketIndex = victim.getAltBucketIndex();
        long replacedTag = table.swapRandomTagInBucket(bucketIndex, victim.getTag());

        int altBucketIndex;
        switch(SHRINK_FILTER_TABLE_MODE){
            case _8_4_4:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 8);
                break;
            case _8_12_12:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 24);
                break;
            case _12_10_10:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 20);
                break;
            case _16_8_8:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 16);
                break;
            default:
                System.out.println("exception...");
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 8);
        }
        if(table.insertToBucket(altBucketIndex, replacedTag)){
            return true;
        }else{
            // update victim
            victim.setBucketIndex(bucketIndex);
            victim.setAltBucketIndex(altBucketIndex);
            victim.setTag(replacedTag);
            return false;
        }
    }

    public boolean query(long ts, long window){
        // according to ts, find whether an interval window includes this ts
        long windowId = ts / window;
        SFIndexAndFingerprint indexAndFingerprint = table.generate(windowId);
        int fingerprint = indexAndFingerprint.fingerprint;
        long distance = ts - windowId * window;
        long tag = getTag(window, fingerprint, distance);
        int bucketIndex = indexAndFingerprint.index;
        int altBucketIndex = table.altIndex(bucketIndex, fingerprint);

        //System.out.println("query tag: " + Long.toHexString(tag) + " | two buckets: " + bucketIndex + ", " + altBucketIndex);
        return table.findTag(bucketIndex, altBucketIndex, tag);
    }

    public boolean queryWindowId(long windowId){
        SFIndexAndFingerprint indexAndFingerprint = table.generate(windowId);
        int fingerprint = indexAndFingerprint.fingerprint;
        int bucketIndex = indexAndFingerprint.index;
        int altBucketIndex = table.altIndex(bucketIndex, fingerprint);
        return table.findFingerprint(bucketIndex, altBucketIndex, fingerprint);
    }

    private long getTag(long window, int fingerprint, long distance) {
        long tag;
        switch (SHRINK_FILTER_TABLE_MODE){
            case _8_12_12:
                tag = (long) fingerprint << 24;
                switch ((int) (distance * 12.0 / window)){
                    case 0:
                        // interval_marker, hit_marker: 0000 0000 0001 0000 0000 0000
                        tag += 0x1000;
                        break;
                    case 1:
                        // interval_marker, hit_marker: 0000 0000 0010 0000 0000 0000
                        tag += 0x2000;
                        break;
                    case 2:
                        // interval_marker, hit_marker: 0000 0000 0100 0000 0000 0000
                        tag += 0x4000;
                        break;
                    case 3:
                        // interval_marker, hit_marker: 0000 0000 1000 0000 0000 0000
                        tag += 0x8000;
                        break;
                    case 4:
                        // interval_marker, hit_marker: 0000 0001 0000 0000 0000 0000
                        tag += 0x10000;
                        break;
                    case 5:
                        // interval_marker, hit_marker: 0000 0010 0000 0000 0000 0000
                        tag += 0x20000;
                        break;
                    case 6:
                        // interval_marker, hit_marker: 0000 0100 0000 0000 0000 0000
                        tag += 0x40000;
                        break;
                    case 7:
                        // interval_marker, hit_marker: 0000 1000 0000 0000 0000 0000
                        tag += 0x80000;
                        break;
                    case 8:
                        // interval_marker, hit_marker: 0001 0000 0000 0000 0000 0000
                        tag += 0x100000;
                        break;
                    case 9:
                        // interval_marker, hit_marker: 0010 0000 0000 0000 0000 0000
                        tag += 0x200000;
                        break;
                    case 10:
                        // interval_marker, hit_marker: 0100 0000 0000 0000 0000 0000
                        tag += 0x400000;
                        break;
                    case 11:
                        // interval_marker, hit_marker: 1000 0000 0000 0000 0000 0000
                        tag += 0x800000;
                        break;
                    default:
                        System.out.println("exception...");
                        tag += 0x800000;
                        break;
                }
                return tag;
            case _12_10_10:
                tag = (long) fingerprint << 20;
                switch((int) (distance * 10.0 / window)){
                    case 0:
                        // interval_marker, hit_marker: 0000 0000 01_00 0000 0000
                        tag += 0x400;
                        break;
                    case 1:
                        // interval_marker, hit_marker:  0000 0000 10_00 0000 0000
                        tag += 0x800;
                        break;
                    case 2:
                        // interval_marker, hit_marker:  0000 0001 00_00 0000 0000
                        tag += 0x1000;
                        break;
                    case 3:
                        // interval_marker, hit_marker:  0000 0010 00_00 0000 0000
                        tag += 0x2000;
                        break;
                    case 4:
                        // interval_marker, hit_marker:  0000 0100 00_00 0000 0000
                        tag += 0x4000;
                        break;
                    case 5:
                        // interval_marker, hit_marker:  0000 1000 00_00 0000 0000
                        tag += 0x8000;
                        break;
                    case 6:
                        // interval_marker, hit_marker:  0001 0000 00_00 0000 0000
                        tag += 0x10000;
                        break;
                    case 7:
                        // interval_marker, hit_marker:  0010 0000 00_00 0000 0000
                        tag += 0x20000;
                        break;
                    case 8:
                        // interval_marker, hit_marker:  0100 0000 00_00 0000 0000
                        tag += 0x40000;
                        break;
                    case 9:
                        // interval_marker, hit_marker:  1000 0000 00_00 0000 0000
                        tag += 0x80000;
                        break;
                    default:
                        System.out.println("exception...");
                        tag += 0x80000;
                }
                return tag;
            case _16_8_8:
                tag = (long) fingerprint << 16;
                switch ((int) (distance * 8.0 / window)){
                    case 0:
                        // interval_marker, hit_marker: 0000 0001 0000 0000
                        tag += 0x100;
                        break;
                    case 1:
                        // interval_marker, hit_marker: 0000 0010 0000 0000
                        tag += 0x200;
                        break;
                    case 2:
                        // interval_marker, hit_marker: 0000 0100 0000 0000
                        tag += 0x400;
                        break;
                    case 3:
                        // interval_marker, hit_marker: 0000 1000 0000 0000
                        tag += 0x800;
                        break;
                    case 4:
                        // interval_marker, hit_marker: 0001 0000 0000 0000
                        tag += 0x1000;
                        break;
                    case 5:
                        // interval_marker, hit_marker: 0010 0000 0000 0000
                        tag += 0x2000;
                        break;
                    case 6:
                        // interval_marker, hit_marker: 0100 0000 0000 0000
                        tag += 0x4000;
                        break;
                    case 7:
                        // interval_marker, hit_marker: 1000 0000 0000 0000
                        tag += 0x8000;
                        break;
                    default:
                        System.out.println("exception...");
                        tag += 0x8000;
                        break;
                }
                return tag;
            default:
                //case: _8_4_4
                tag = (long) fingerprint << 8;
                if(distance < (window >> 1)){
                    if(distance < (window >> 2)){
                        // interval_marker, hit_marker: 0001 0000
                        tag += 0x10;
                    }else{
                        // interval_marker, hit_marker: 0010 0000
                        tag += 0x20;
                    }
                }else{
                    if(distance < (window - (window >> 1) + (window >> 2))){
                        // interval_marker, hit_marker: 0100 0000
                        tag += 0x40;
                    }else{
                        // interval_marker, hit_marker: 1000 0000
                        tag += 0x80;
                    }
                }
                return tag;
        }
    }

    /**
     * when querying returns true, we need to update hit_markers
     * we need to ensure this key exists
     *
     * @param startTs - start timestamp
     * @param endTs   - end timestamp
     * @param window  - query window
     */
    public void updateRange(long startTs, long endTs, long window){
        // only use for debugging
        //if(startTs > endTs){
        //    throw new RuntimeException("start timestamp ("+ startTs + ") is greater than end timestamp (" + endTs + ")");
        //}
        long key = startTs / window;
        long[] intervalMarkers = getIntervalMarkers(startTs, endTs, window);
        int len = intervalMarkers.length;

        for(int i = 0; i < len; i++){
            SFIndexAndFingerprint ip = table.generate(key + i);
            int bucketIndex = ip.index;
            int altBucketIndex  = table.altIndex(bucketIndex, ip.fingerprint);
            long tag;
            switch(SHRINK_FILTER_TABLE_MODE){
                case _8_4_4:
                    tag = ((long) ip.fingerprint << 8) + (intervalMarkers[i] << 4);
                    break;
                case _8_12_12:
                    tag = ((long) ip.fingerprint << 24) + (intervalMarkers[i] << 12);
                    break;
                case _12_10_10:
                    tag = ((long) ip.fingerprint << 20) + (intervalMarkers[i] << 10);
                    break;
                default:
                    //case _16_8_8:
                    tag = ((long) ip.fingerprint << 16) + (intervalMarkers[i] << 8);
                    break;
            }
            table.updateTagInBucket(bucketIndex, altBucketIndex, tag);
        }
    }

    public static long[] getIntervalMarkers(long startTs, long endTs, long window){
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;
        long offset = startWindowId * window;
        // in fact, endTs >= startTs + window
        // thus the first if condition (endWindowId == startWindowId) can be removed
        // here we use this condition (endWindowId == startWindowId) only for testing
        if(endWindowId == startWindowId) {
            int intervalMarker = getLeftIntervalMarker(startTs, offset, window) &
                    getRightIntervalMarker(endTs, offset, window);
            return new long[]{intervalMarker};
        }

        // we split the range into three parts:
        // startWindowId, [startWindowId + 1, endWindowId - 1], endWindowId

        // first part
        long[] ans = new long[(int) (endWindowId - startWindowId + 1)];
        int cnt = 0;
        ans[cnt++] = getLeftIntervalMarker(startTs, offset, window);

        // second part
        for(long key = startWindowId + 1; key < endWindowId; ++key){
            switch(SHRINK_FILTER_TABLE_MODE){
                case _8_4_4:
                    ans[cnt++] = 0x0f;
                    break;
                case _8_12_12:
                    ans[cnt++] = 0xfff;
                    break;
                case _12_10_10:
                    ans[cnt++] = 0x3ff;
                    break;
                case _16_8_8:
                    ans[cnt++] = 0x0ff;
                    break;
            }
        }
        ans[cnt] = getRightIntervalMarker(endTs, endWindowId * window, window);
        return ans;
    }

    // due to hash collision and false positive rate, this is approximate estimation
    public double getApproximateWindowNum(){
        return table.getApproximateWindowNum();
    }


    public void merge(ShrinkFilter wcf){
        table.merge(wcf.getTable());
    }

    /**
     * insert -> update -> rebuild
     * this functions aims to delete the intervals that don't contain matched results
     */
    public void rebuild(){
        int keyNum = table.rebuildTable();

        // check whether we need to trigger the compact operation
        // please note that the compact operation will increase false positive rate

        //System.out.println("start compress...");
        //display();
        //System.out.println("=====================================");
        // debug condition: keyNum / (bucketNum * 4.0) < LOAD_FACTOR * 0.5 && bucketNum >= 16

        while(keyNum / (bucketNum * 4.0) < ShrinkFilter.LOAD_FACTOR * 0.5 && bucketNum >= 4){
            // this function aims to rollback
            AbstractSFTable copiedTable = table.copy();
            bucketNum = bucketNum >> 1;
            // firstly, we get the contents of the second half of the bucket
            // high 32 bits in long value is fingerprint, low 32 bits are tag
            long[][] semi_bucket_fp_tags = table.compact();
            // then we try inset these tags, if we cannot insert successfully, we need rollback
            for(int i = 0; i < bucketNum; ++i){
                for(int slotId = 0; slotId < 4; ++slotId){
                    long fp_tag = semi_bucket_fp_tags[i][slotId];
                    // here a tag len < 32
                    int altIndex = table.altIndex(i, (fp_tag >> 32) & 0xffffffffL);
                    if(fp_tag == 0){
                        // early stop, because non-zero values always before zero values
                        break;
                    }else{
                        if(!put(i, altIndex, fp_tag & 0xffffffffL)){
                            // cannot compact, we need to rollback
                            table = copiedTable;
                            bucketNum = bucketNum << 1;
                            // compactFlag = false
                            return;
                        }
                    }
                }
            }
        }
        //System.out.println("end compress...");
        //display();
    }

    public AbstractSFTable getTable(){
        return table;
    }

    public ByteBuffer serialize(){
        return table.serialize();
    }

    public static ShrinkFilter deserialize(ByteBuffer buffer){
        // ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int arrayLen = buffer.getInt();
        long[] bitSet = new long[arrayLen];
        // we cannot call this function: buffer.get(byte[] ...)
        for(int i = 0; i < arrayLen; ++i){
            long bucketContent = buffer.getLong();
            bitSet[i] = bucketContent;
        }

        switch (SHRINK_FILTER_TABLE_MODE){
            case _8_4_4:
                return new ShrinkFilter.Builder(SFTable8_4_4.create(bitSet)).build();
            case _8_12_12:
                return new ShrinkFilter.Builder(SFTable8_12_12.create(bitSet)).build();
            case _12_10_10:
                return new ShrinkFilter.Builder(SFTable12_10_10.create(bitSet)).build();
            case _16_8_8:
                return new ShrinkFilter.Builder(SFTable16_8_8.create(bitSet)).build();
            default:
                System.out.println("exception...");
                return new ShrinkFilter.Builder(SFTable8_4_4.create(bitSet)).build();
        }
    }

    // copy function
    public ShrinkFilter copy(){
        AbstractSFTable copiedTable = this.getTable().copy();
        return new ShrinkFilter.Builder(copiedTable).build();
    }

    // this function used for debugging
    public void display(){
        System.out.println("bucket num:" + bucketNum);
        table.displayWithHex();
    }
}

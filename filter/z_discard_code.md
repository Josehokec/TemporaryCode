

这个地方的代码是8_24配置的，fpr最小时

```java
package filter;


import java.nio.ByteBuffer;

public class ShrinkFilterUltra {
    // note that original paper MAX_KICK_OUT = 500, due to we swap choose 4 position to kick,
    // to achieve a high load factor, we enlarge MAX_KICK_OUT to 550
    static final int MAX_KICK_OUT = 550;
    private static final double LOAD_FACTOR = 0.955;
    private AbstractSFTableV2 table;
    private int bucketNum;

    // 0, 1, 2, 3, ..., 23
    private static final int[] leftIntervalMarkerArray = {
            0xffffff, 0xfffffe, 0xfffffc, 0xfffff8,
            0xfffff0, 0xffffe0, 0xffffc0, 0xffff80,
            0xffff00, 0xfffe00, 0xfffc00, 0xfff800,
            0xfff000, 0xffe000, 0xffc000, 0xff8000,
            0xff0000, 0xfe0000, 0xfc0000, 0xf80000,
            0xf00000, 0xe00000, 0xc00000, 0x800000
    };

    // 0, 1, 2, 3, ..., 23
    private static final int[] rightIntervalMarkerArray = {
            0x000001, 0x000003, 0x000007, 0x00000f,
            0x00001f, 0x00003f, 0x00007f, 0x0000ff,
            0x0001ff, 0x0003ff, 0x0007ff, 0x000fff,
            0x001fff, 0x003fff, 0x007fff, 0x00ffff,
            0x01ffff, 0x03ffff, 0x07ffff, 0x0fffff,
            0x1fffff, 0x3fffff, 0x7fffff, 0xffffff
    };

    private static final int[] tags = {
            0x000001, 0x000002, 0x000004, 0x000008,
            0x000010, 0x000020, 0x000040, 0x000080,
            0x000100, 0x000200, 0x000400, 0x000800,
            0x001000, 0x002000, 0x004000, 0x008000,
            0x010000, 0x020000, 0x040000, 0x080000,
            0x100000, 0x200000, 0x400000, 0x800000
    };

    private ShrinkFilterUltra(AbstractSFTableV2 table, int bucketNum) {
        this.table = table;
        this.bucketNum = bucketNum;
    }

    public static class Builder {
        private final long maxKeys;
        private AbstractSFTableV2 table;

        //private int bitConfig;
        public Builder(long maxKeys) {
            this.maxKeys = maxKeys;
            table = null;
        }

        public Builder(AbstractSFTableV2 table) {
            maxKeys = -1;
            this.table = table;
        }
        
        public ShrinkFilterUltra build() {
            if (table != null) {
                int bucketNum = table.getBucketNum();
                return new ShrinkFilterUltra(table, bucketNum);
            } else {
                int bucketNum = CFUtils.getBucketsNeeded(maxKeys, LOAD_FACTOR, 4);
                table = SFTable12_20.createTable(bucketNum);
                return new ShrinkFilterUltra(table, bucketNum);
            }
        }
    }

    static class SFVictim {
        private int bucketIndex;
        private int altBucketIndex;
        private long tag;

        //WCFVictim(){ tag = -1; }

        SFVictim(int bucketIndex, int altBucketIndex, long tag) {
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

    public int getBucketNum() {
        return bucketNum;
    }

    /**
     * insert a time interval
     * @param startTs - start timestamp
     * @param endTs - end timestamp
     * @param window - query window
     * @return - true if insert successfully
     */
    public boolean insert(long startTs, long endTs, long window) {
        // below three lines only used for debugging
        if (startTs > endTs) {
            throw new RuntimeException("start timestamp (" + startTs + ") greater than end timestamp (" + endTs + ")");
        }

        long key = startTs / window;
        long[] intervalMarkers = getIntervalMarkers(startTs, endTs, window);
        int len = intervalMarkers.length;
        for (int i = 0; i < len; i++) {
            // fingerprint: low 8 bits
            SFIndexAndFingerprint ip = table.generate(key + i);
            int bucketIndex = ip.index;
            long fp = ip.fingerprint;
            long tag = (fp << 24) + intervalMarkers[i];
            int altBucketIndex = table.altIndex(bucketIndex, ip.fingerprint);

            //System.out.println("key: " + startTs + " i1: " + bucketIndex + " i2: " + altBucketIndex +
            //        " fp: " + Long.toHexString(ip.fingerprint) + " tag: " + Long.toHexString(tag));

            int[] bucketIndexAndSlotIndex = table.findFingerprint(bucketIndex, altBucketIndex, ip.fingerprint);
            if (bucketIndexAndSlotIndex[0] == -1) {
                if (!put(bucketIndex, altBucketIndex, tag)) {
                    return false;
                }
            } else {
                if (!putStar(bucketIndexAndSlotIndex[0], bucketIndexAndSlotIndex[1], tag)) {
                    return false;
                }
            }

        }
        return true;
    }

    public static int getLeftIntervalMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        // due to we default use 16 bits as interval marker and hit marker to quickly decode
        int ptr = (int) (distance * 24.0 / window);
        return leftIntervalMarkerArray[ptr];
    }

    public static int getRightIntervalMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 24.0 / window);
        return rightIntervalMarkerArray[ptr];
    }

    public static long[] getIntervalMarkers(long startTs, long endTs, long window) {
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;
        long offset = startWindowId * window;
        // in fact, endTs >= startTs + window
        // thus the first if condition (endWindowId == startWindowId) can be removed
        // here we use this condition (endWindowId == startWindowId) only for testing
        if (endWindowId == startWindowId) {
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
        for (long key = startWindowId + 1; key < endWindowId; key++) {
            ans[cnt++] = 0xffffff;
        }
        // third part
        ans[cnt] = getRightIntervalMarker(endTs, endWindowId * window, window);
        return ans;
    }

    private boolean put(int bucketIndex, int altBucketIndex, long tag) {
        if (table.insertToBucket(bucketIndex, tag) || table.insertToBucket(altBucketIndex, tag)) {
            return true;
        }
        // we need to kick
        int kickNum = 0;
        SFVictim victim = new SFVictim(bucketIndex, altBucketIndex, tag);
        while (kickNum < MAX_KICK_OUT) {
            if (randomSwap(victim)) {
                break;
            }
            kickNum++;
        }

        return kickNum != MAX_KICK_OUT;
    }

    private boolean putStar(int bucketIndex, int slotIndex, long tag) {
        // we have lock this
        return table.insertToBucket(bucketIndex, slotIndex, tag);
    }

    private boolean randomSwap(SFVictim victim) {
        // always choose alt bucket index
        int bucketIndex = victim.getAltBucketIndex();
        long replacedTag = table.swapRandomTagInBucket(bucketIndex, victim.getTag());
        int altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 24);

        if (table.insertToBucket(altBucketIndex, replacedTag)) {
            return true;
        } else {
            // update victim
            victim.setBucketIndex(bucketIndex);
            victim.setAltBucketIndex(altBucketIndex);
            victim.setTag(replacedTag);
            return false;
        }
    }

    public boolean query(long ts, long window) {
        long windowId = ts / window;
        SFIndexAndFingerprint indexAndFingerprint = table.generate(windowId);
        int fingerprint = indexAndFingerprint.fingerprint;
        long distance = ts - windowId * window;
        // interval_marker occupies 24 bits
        int ptr = (int) (distance * 24.0 / window);
        long tag = tags[ptr] + ((long) fingerprint << 24);
        int bucketIndex = indexAndFingerprint.index;
        int altBucketIndex = table.altIndex(bucketIndex, fingerprint);
        return table.findTag(bucketIndex, altBucketIndex, tag);
    }

    // before updating range, we call this function to check window id
    // if return {-1,-1}, then we do not update
    // return bucket position and slot position
    public int[] queryWindowId(long windowId) {
        SFIndexAndFingerprint indexAndFingerprint = table.generate(windowId);
        int fingerprint = indexAndFingerprint.fingerprint;
        int bucketIndex = indexAndFingerprint.index;
        int altBucketIndex = table.altIndex(bucketIndex, fingerprint);
        return table.findFingerprint(bucketIndex, altBucketIndex, fingerprint);
    }

    // due to hash collision and false positive rate, this is approximate estimation
    public double getApproximateWindowNum() {
        return table.getApproximateWindowNum();
    }

    public void rebuild(SFHitMarkers sfHitMarkers) {
        int keyNum = table.rebuildTable(sfHitMarkers);

        while (keyNum / (bucketNum * 4.0) < ShrinkFilterUltra.LOAD_FACTOR * 0.5 && bucketNum > 4) {
            // this function aims to rollback
            AbstractSFTableV2 copiedTable = table.copy();
            bucketNum = bucketNum >> 1;
            // firstly, we get the contents of the second half of the bucket
            // high 32 bits in long value is fingerprint, low 32 bits are tag
            long[][] semi_bucket_fp_tags = table.compact();
            // then we try inset these tags, if we cannot insert successfully, we need rollback
            for (int i = 0; i < bucketNum; ++i) {
                for (int slotId = 0; slotId < 4; ++slotId) {
                    long fp_tag = semi_bucket_fp_tags[i][slotId];
                    // here a tag len < 32
                    int altIndex = table.altIndex(i, (fp_tag >> 32) & 0xffL);
                    if (fp_tag == 0) {
                        // early stop, because non-zero values always before zero values
                        break;
                    } else {
                        if (!put(i, altIndex, fp_tag & 0xffffffffL)) {
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
    }

    public ByteBuffer serialize() {
        return table.serialize();
    }

    public static ShrinkFilterUltra deserialize(ByteBuffer buffer) {
        int arrayLen = buffer.getInt();
        long[] bitSet = new long[arrayLen];
        // we cannot call this function: buffer.get(byte[] ...)
        for (int i = 0; i < arrayLen; ++i) {
            long bucketContent = buffer.getLong();
            bitSet[i] = bucketContent;
        }
        return new ShrinkFilterUltra.Builder(SFTable12_20.createTable(bitSet)).build();
    }

    // this function used for debugging
    public void display() {
        System.out.println("bucket num:" + bucketNum);
        table.displayWithHex();
    }
}

```


这里是对应的table
```java
package filter;

import java.nio.ByteBuffer;

/**
 * this table only contains fingerprint and interval_markers
 */
public abstract class AbstractSFTableV2 {
    abstract int getBucketNum();
    abstract int getBucketByteSize();

    abstract long getLongArrayContent(int arrayPos);
    abstract ByteBuffer serialize();

    /**
     * note that:
     *   1. if two item have same key, then we will merge them
     *   2. if two item have different key, but their hash value is same, we also merge them
     * insert tag, note that fingerprint can be zero, interval_marker cannot be zero
     * @param bucketIndex - bucketIndex
     * @param tag - (fingerprint, interval_marker, hit_marker)
     */
    abstract boolean insertToBucket(int bucketIndex, long tag);

    abstract boolean insertToBucket(int bucketIndex, int slotPos, long tag);

    /**
     * here interval_marker only one bit with a value of 1, hit_marker = 0
     * @param i1 - bucket index 1
     * @param i2 - bucket index 2
     * @param tag - (fingerprint, interval_marker in {1,2,4,8}, hit_marker = 0)
     * @return - fingerprint exists and interval_marker & corresponding position not zero
     */
    abstract boolean findTag(int i1, int i2, long tag);

    /**
     * find bucket and slot position, then return
     * @param i1 - first bucket index
     * @param i2 - second bucket index
     * @param fp - fingerprint
     * @return bucket position and slot position
     */
    abstract int[] findFingerprint(int i1, int i2, long fp);

    abstract int altIndex(int bucketIndex, long fingerprint);

    abstract SFIndexAndFingerprint generate(long item);

    abstract long swapRandomTagInBucket(int bucketIndex, long tag);

    abstract void updateTagInBucket(SFHitMarkers sfMarkers);

    abstract double getApproximateWindowNum();

    /**
     * this functions aims to delete the intervals that don't contain matched results
     * steps: insert -> update -> rebuild
     * @param markers - hit_markers
     * @return - number of keys
     */
    abstract int rebuildTable(SFHitMarkers markers);

    abstract long[][] compact();

    // this function is used for debugging
    abstract void displayWithDecimal();

    // this function is used for debugging
    abstract void displayWithHex();

    abstract AbstractSFTableV2 copy();
}

```


小表
```java
package filter;

import java.nio.ByteBuffer;

/**
 * the length of a hit_marker is 24
 * a bucket has four hit_markers
 * then a bucket has 96 bits = 1.5 long value (1.5 * 64 = 96)
 * in the future, we will define a base class to support different length setting
 */
public class SFHitMarkers {

    private final long[] bitSet;

    public SFHitMarkers(int bucketNum){
        bitSet = new long[(bucketNum * 3) >> 1];
    }

    public int getBucketNum(){
        return bitSet.length * 2 / 3;
    }

    public SFHitMarkers(long[] bitSet){
        this.bitSet = bitSet;
    }

    public void updateMarker(int bucketIndex, int slotIndex, long marker){
        if(marker >= 0x1000000){
            throw new RuntimeException("wrong marker: marker = " + marker);
        }

        int writePos = (bucketIndex * 3) >> 1;
        if((bucketIndex & 0x1) == 0){
            switch (slotIndex){
                case 0:
                    bitSet[writePos] = bitSet[writePos] | marker;
                    break;
                case 1:
                    bitSet[writePos] = bitSet[writePos] | (marker << 24);
                    break;
                case 2:
                    bitSet[writePos] = bitSet[writePos] | (marker << 48);
                    bitSet[writePos + 1] = bitSet[writePos + 1] | (marker >> 16);
                    break;
                case 3:
                    bitSet[writePos + 1] = bitSet[writePos + 1] | (marker << 8);
                    break;
            }
        }else{
            switch(slotIndex){
                case 0:
                    bitSet[writePos] = bitSet[writePos] | (marker << 32);
                    break;
                case 1:
                    bitSet[writePos] = bitSet[writePos] | (marker << 56);
                    bitSet[writePos + 1] = bitSet[writePos + 1] | (marker >> 8);
                    break;
                case 2:
                    bitSet[writePos + 1] = bitSet[writePos + 1] | (marker << 16);
                    break;
                case 3:
                    bitSet[writePos + 1] = bitSet[writePos + 1] | (marker << 40);
                    break;
            }
        }
    }

    public long getLongArrayContent(int arrayPos){
        return bitSet[arrayPos];
    }

    public void merge(SFHitMarkers other){
        int len = bitSet.length;
        for (int i = 0; i < len; i++){
            bitSet[i] |= other.getLongArrayContent(i);
        }
    }

    public long[] getMarkers(int bucketIndex){
        int pos= (bucketIndex * 3) >> 1;
        if((bucketIndex & 0x1) == 0){
            long slot0 = bitSet[pos] & 0xffffff;
            long slot1 = (bitSet[pos] >> 24) & 0xffffff;
            long slot2 = ((bitSet[pos] >> 48) & 0xffff) + ((bitSet[pos + 1] & 0xffff) << 16);
            long slot3 = (bitSet[pos + 1] >> 8) & 0xffffff;
            return new long[]{slot0, slot1, slot2, slot3};
        }else{
            long slot0 = (bitSet[pos] >> 32) & 0xffffff;
            long slot1 = ((bitSet[pos] >> 56) & 0xff) + ((bitSet[pos + 1] & 0xffff) << 8);
            long slot2 = (bitSet[pos + 1] >> 16) & 0xffffff;
            long slot3 = (bitSet[pos + 1] >> 40) & 0xffffff;
            return new long[]{slot0, slot1, slot2, slot3};
        }
    }

    ByteBuffer serialize(){
        ByteBuffer buffer = ByteBuffer.allocate(bitSet.length << 3);
        for (long val : bitSet) {
            buffer.putLong(val);
        }
        buffer.flip();
        return buffer;
    }

    static SFHitMarkers deserialize(ByteBuffer buffer){
        int cap = buffer.capacity() >> 3;
        long[] longVals = new long[cap];
        for(int i = 0; i < cap; i++){
            longVals[i] = buffer.getLong();
        }
        return new SFHitMarkers(longVals);
    }

    public void display(){
        int bucketNum = getBucketNum();
        System.out.println("----------------SFHitMarkers----------------");
        for(int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++){

            System.out.print(bucketIndex + "-th bucket:");
            long[] markers = getMarkers(bucketIndex);
            System.out.print(" 0x" + Long.toHexString(markers[0]));
            System.out.print(" 0x" + Long.toHexString(markers[1]));
            System.out.print(" 0x" + Long.toHexString(markers[2]));
            System.out.print(" 0x" + Long.toHexString(markers[3]));
            System.out.println();
        }
        System.out.println("--------------------------------------------");
    }
}

```

具体的table，8——24的配置

```java
package filter;

import hasher.QuickHash;
import hasher.XXHash;
import utils.BasicTypeToByteUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 8_24 means len(fingerprint) = 8 bits, len(interval_marker) = 24 bits
 * we choose this setting because it will have high update throughput
 */
public class SFTable8_24 extends AbstractSFTableV2 {
    private long[] bitSet;

    private SFTable8_24(int bucketNum) {
        // a bucket occupies 128 bits
        bitSet = new long[bucketNum << 1];
    }

    private SFTable8_24(long[] bitSet) {
        this.bitSet = bitSet;
    }

    static SFTable12_20 createTable(int bucketNum) {
        // only used for debugging
        if ((bucketNum & (bucketNum - 1)) != 0) {
            throw new IllegalArgumentException("bucketNum must be a power of 2, current bucketNum = " + bucketNum);
        }
        return new SFTable12_20(bucketNum);
    }

    static SFTable12_20 createTable(long[] bitSet) {
        return new SFTable12_20(bitSet);
    }

    @Override
    int getBucketNum() {
        return bitSet.length >> 1;
    }

    @Override
    int getBucketByteSize() {
        return 16;
    }

    @Override
    long getLongArrayContent(int arrayPos) {
        return bitSet[arrayPos];
    }

    @Override
    ByteBuffer serialize() {
        int bucketNum = getBucketNum();
        int bucketSumSize = bucketNum * getBucketByteSize();
        ByteBuffer buffer = ByteBuffer.allocate(bucketSumSize + 4);
        buffer.putInt(bucketNum);
        for (long val : bitSet) {
            buffer.putLong(val);
        }
        buffer.flip();
        return buffer;
    }

    // you should first call findTag() function to get the bucket index
    // then call insertToBucket function
    @Override
    boolean insertToBucket(int bucketIndex, long tag) {
        // here tag = fingerprint + interval_marker
        // below 3 code lines only used for debugging
        if ((tag & 0xffffff) == 0 || (tag > 0xffffffffL)) {
            throw new RuntimeException("tag (" + Long.toBinaryString(tag) + ") is illegal.");
        }
        int pos = bucketIndex << 1;
        // due to we have call findFingerprint function,
        // we know without same fingerprint, then we only
        // need to find a vacant slot (i.e., intervalMarker = 0)
        for (int idx : new int[]{pos, pos + 1}) {
            long val = bitSet[idx];
            if ((val & 0xffffff) == 0) {
                bitSet[idx] |= tag;
                return true;
            }
            if (((val >> 32) & 0xffffff) == 0) {
                bitSet[idx] |= (tag << 32);
                return true;
            }
        }
        return false;
    }

    @Override
    boolean insertToBucket(int bucketIndex, int slotPos, long tag) {
        // slotPos: 0, 1, 2, 3
        switch (slotPos) {
            case 0:
                bitSet[bucketIndex << 1] |= tag;
                return true;
            case 1:
                bitSet[bucketIndex << 1] |= (tag << 32);
                return true;
            case 2:
                bitSet[(bucketIndex << 1) + 1] |= tag;
                return true;
            case 3:
                bitSet[(bucketIndex << 1) + 1] |= (tag << 32);
                return true;
        }
        return false;
    }

    @Override
    boolean findTag(int i1, int i2, long tag) {
        // last 8 bits should be 0, and 9~16bits cannot be 0
        if (tag > 0xffffffffL || (((tag & 0xffffffL) & ((tag & 0xffffffL) - 1)) != 0)) {
            throw new RuntimeException("tag (" + Long.toBinaryString(tag) + ") is illegal.");
        }

        long intervalMarker = tag & 0xffffffL;
        long mask = (intervalMarker << 32) + intervalMarker + 0xff_000000_ff_000000L;

        int pos1 = i1 << 1;
        int pos2 = i2 << 1;
        return hasValue32Bits(bitSet[pos1] & mask, tag) || hasValue32Bits(bitSet[pos1 + 1] & mask, tag) ||
                hasValue32Bits(bitSet[pos2] & mask, tag) || hasValue32Bits(bitSet[pos2 + 1] & mask, tag);
    }

    private static long hasZero32Bits(long x) {
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (((x) - 0x0000_0001_0000_0001L) & (~(x)) & 0x8000_0000_8000_0000L);
    }

    private static boolean hasValue32Bits(long x, long n) {
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (hasZero32Bits((x) ^ (0x0000_0001_0000_0001L * (n)))) != 0;
    }

    @Override
    int[] findFingerprint(int i1, int i2, long fp) {
        for (int bucketIndex : new int[]{i1, i2}) {
            int arrayPos = bucketIndex << 1;
            if (bitSet[arrayPos] != 0) {
                long fp0 = (bitSet[arrayPos] >> 24) & 0xff;
                long intervalMarker0 = bitSet[arrayPos] & 0xffffff;
                long fp1 = (bitSet[arrayPos] >> 56) & 0xff;
                long intervalMarker1 = (bitSet[arrayPos] >> 32) & 0xffffff;
                if (fp0 == fp && intervalMarker0 != 0) {
                    return new int[]{bucketIndex, 0};
                }
                if (fp1 == fp && intervalMarker1 != 0) {
                    return new int[]{bucketIndex, 1};
                }

                if (bitSet[arrayPos + 1] != 0) {
                    long fp2 = (bitSet[arrayPos + 1] >> 24) & 0xff;
                    long intervalMarker2 = bitSet[arrayPos + 1] & 0xffffff;
                    long fp3 = (bitSet[arrayPos + 1] >> 56) & 0xff;
                    long intervalMarker3 = (bitSet[arrayPos + 1] >> 32) & 0xffffff;

                    if (fp2 == fp && intervalMarker2 != 0) {
                        return new int[]{bucketIndex, 2};
                    }
                    if (fp3 == fp && intervalMarker3 != 0) {
                        return new int[]{bucketIndex, 3};
                    }
                }
            }
        }
        return new int[]{-1, -1};
    }

    @Override
    int altIndex(int bucketIndex, long fingerprint) {
        long altIndex = bucketIndex ^ ((fingerprint + 1) * 0xc4ceb9fe1a85ec53L);
        // now pull into valid range
        return hashIndex(altIndex);
    }

    @Override
    SFIndexAndFingerprint generate(long item) {
//        long hashCode = XXHash.hash64(BasicTypeToByteUtils.longToBytes(item), 8, 0);
//        int fingerprint = (int) (hashCode & 0xff);
//        int bucketIndex = hashIndex(hashCode >> 8);
        long hashCode = QuickHash.hash64(item);
        int fingerprint = (int) ((hashCode >> 56) & 0xff);
        int bucketIndex = hashIndex(hashCode);
        return new SFIndexAndFingerprint(bucketIndex, fingerprint);
    }

    final int hashIndex(long originIndex) {
        // we always need to return a bucket index within table range
        // we can return low bit because numBuckets is a pow of two
        return (int) (originIndex & ((bitSet.length >> 1) - 1));
    }

    @Override
    long swapRandomTagInBucket(int bucketIndex, long tag) {
        // generate random position from {0, 1, 2, 3}, 3 has bug
        int randomSlotPosition = ThreadLocalRandom.current().nextInt(4);
        long returnTag;
        int writePos;
        //System.out.println("swap tag: " + Long.toHexString(tag) + " swap pos: " +  randomSlotPosition);
        switch (randomSlotPosition) {
            case 0:
                writePos = bucketIndex << 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff00000000L) | tag;
                break;
            case 1:
                writePos = bucketIndex << 1;
                returnTag = (bitSet[writePos] >> 32) & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0x00000000ffffffffL) | (tag << 32);
                break;
            case 2:
                writePos = (bucketIndex << 1) + 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff00000000L) | tag;
                break;
            default:
                // case 3
                writePos = (bucketIndex << 1) + 1;
                returnTag = (bitSet[writePos] >> 32) & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0x00000000ffffffffL) | (tag << 32);
        }
        //System.out.println("returnTag ==> 0x" + Long.toHexString(returnTag));
        return returnTag;
    }

    @Override
    void updateTagInBucket(SFHitMarkers sfMarkers) {
        int bucketNum = getBucketNum();
        for (int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++) {
            long[] markers = sfMarkers.getMarkers(bucketIndex);
            int pos = bucketIndex << 1;
            long mask1 = 0xff_000000_ff_000000L + markers[0] + (markers[1] << 32);
            long mask2 = 0xff_000000_ff_000000L + markers[2] + (markers[3] << 32);
            bitSet[pos] &= mask1;
            bitSet[pos + 1] &= mask2;
        }
    }

    @Override
    double getApproximateWindowNum() {
        int count = 0;
        for (long val : bitSet) {
            count += Long.bitCount(val & 0x00ffffff00ffffffL);
        }
        return count / 24.0;
    }

    @Override
    int rebuildTable(SFHitMarkers sfHitMarkers) {
        int tagNum = 0;
        int bucketNum = getBucketNum();

        long[] slots = new long[4];
        for (int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++) {
            int pos = bucketIndex << 1;
            slots[0] = bitSet[pos] & 0xffffffffL;
            slots[1] = (bitSet[pos] >> 32) & 0xffffffffL;
            slots[2] = bitSet[pos + 1] & 0xffffffffL;
            slots[3] = (bitSet[pos + 1] >> 32) & 0xffffffffL;

            int cnt = 0;
            long[] fourMarkers = sfHitMarkers.getMarkers(bucketIndex);

            for (int idx = 0; idx < 4; idx++) {
                // please modify
                long newIntervalMaker = slots[idx] & fourMarkers[idx];
                if (newIntervalMaker != 0) {
                    // please modify
                    slots[cnt] = (slots[idx] & 0xff000000L) + newIntervalMaker;
                    cnt++;
                }
            }

            tagNum += cnt;
            switch (cnt) {
                case 0:
                    bitSet[pos] = 0;
                    bitSet[pos + 1] = 0;
                    break;
                case 1:
                    bitSet[pos] = slots[0];
                    bitSet[pos + 1] = 0;
                    break;
                case 2:
                    bitSet[pos] = (slots[1] << 32) | slots[0];
                    bitSet[pos + 1] = 0;
                    break;
                case 3:
                    bitSet[pos] = (slots[1] << 32) | slots[0];
                    bitSet[pos + 1] = slots[2];
                    break;
                case 4:
                    bitSet[pos] = (slots[1] << 32) | slots[0];
                    bitSet[pos + 1] = (slots[3] << 32) | slots[2];
            }
        }

        return tagNum;
    }

    @Override
    long[][] compact() {
        int longValueNum = bitSet.length;
        // high 32 bits: fingerprint, low 32 bits: tag
        int originalBucketNum = longValueNum >> 1;
        int newBucketNum = longValueNum >> 2;
        long[][] fp_tags = new long[newBucketNum][4];
        for (int i = newBucketNum; i < originalBucketNum; ++i) {
            int bucketPos = i - newBucketNum;
            int pos = i << 1;
            long val1 = bitSet[pos];
            long val2 = bitSet[pos + 1];
            if (val1 != 0) {
                long firstTag = val1 & 0xffffffffL;
                long secondTag = (val1 >> 32) & 0xffffffffL;
                long thirdTag = val2 & 0xffffffffL;
                long fourthTag = (val2 >> 32) & 0xffffffffL;
                // please modify below four lines
                fp_tags[bucketPos][0] = firstTag + ((firstTag >> 24) << 32);
                fp_tags[bucketPos][1] = secondTag + ((secondTag >> 24) << 32);
                fp_tags[bucketPos][2] = thirdTag + ((thirdTag >> 24) << 32);
                fp_tags[bucketPos][3] = fourthTag + ((fourthTag >> 24) << 32);
            }
        }
        // compact
        long[] newBitSet = new long[longValueNum >> 1];
        System.arraycopy(bitSet, 0, newBitSet, 0, longValueNum >> 1);
        bitSet = newBitSet;
        return fp_tags;
    }

    @Override
    void displayWithDecimal() {
        int bucketNum = (bitSet.length >> 1);
        for (int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++) {
            System.out.print(bucketIndex + "-th bucket:");
            long val1 = bitSet[bucketIndex << 1];
            long val2 = bitSet[(bucketIndex << 1) + 1];
            for (long val : new long[]{val1, val2}) {
                long lowFingerprint = (val >> 24) & 0xff;
                long lowIntervalMarker = val & 0xffffffffL;
                System.out.print(" (" + lowFingerprint + "," + lowIntervalMarker + ")");
                long highFingerprint = (val >> 56) & 0xff;
                long highIntervalMarker = (val >> 32) & 0xffffffffL;
                System.out.println(" (" + highFingerprint + "," + highIntervalMarker + "," + ")");
            }
            System.out.println();
        }
    }

    @Override
    void displayWithHex() {
        int bucketNum = (bitSet.length >> 1);
        for (int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++) {
            System.out.print(bucketIndex + "-th bucket:");
            int pos = bucketIndex << 1;
            long val1 = bitSet[pos];
            long val2 = bitSet[pos + 1];
            System.out.print(" 0x" + Long.toHexString(val1 & 0xffffffffL));
            System.out.print(" 0x" + Long.toHexString((val1 >> 32) & 0xffffffffL));
            System.out.print(" 0x" + Long.toHexString(val2 & 0xffffffffL));
            System.out.print(" 0x" + Long.toHexString((val2 >> 32) & 0xffffffffL));
            System.out.println();
        }
    }

    @Override
    AbstractSFTableV2 copy() {
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable12_20(copyBitSet);
    }
}

```


8-16配置
```java
package filter;

import utils.BasicTypeToByteUtils;

import java.nio.ByteBuffer;

/**
 * slot content: fingerprint (8 bits), interval_marker (16 bits)
 * a bucket has 4 slot, a slot has 24 bits, then a bucket has 96 bits
 * then two buckets has 192 bits, we can use three long value (64 bits) to represent them
 * fpr_{8_16} = 8/256 + 1/16 = 3/32
 */
public class SFTable8_16 extends AbstractSFTableV2{
    private long[] bitSet;

    private SFTable8_16(int bucketNum){
        int longValueNum = (bucketNum >> 1) * 3;
        bitSet = new long[longValueNum];
    }

    private SFTable8_16(long[] bitSet){
        this.bitSet = bitSet;
    }

    static SFTable8_16 createTable(int bucketNum){
        // only used for debugging
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new IllegalArgumentException("bucketNum must be a power of 2, current bucketNum = " + bucketNum);
        }
        return new SFTable8_16(bucketNum);
    }

    static SFTable8_16 create(long[] bitSet){
        // only used for debugging
        if(bitSet.length / 6 != 0){
            throw new IllegalArgumentException("bitSet.length = " + bitSet.length + ", bitSet.length / 6 = " + bitSet.length);
        }
        return new SFTable8_16(bitSet);
    }

    int getBucketNum() {
        // 3 long values == 2 buckets
        return (bitSet.length << 1) / 3 ;
    }

    int getBucketByteSize() {
        return 12;
    }

    long getLongArrayContent(int arrayPos) {
        return bitSet[arrayPos];
    }

    ByteBuffer serialize() {
        // we only need to store table and bucketNum
        // if we support more table, we need to store table version
        int bucketNum = getBucketNum();
        int bucketSumSize = bucketNum * getBucketByteSize();
        ByteBuffer buffer = ByteBuffer.allocate(bucketSumSize + 4);

        // first write long array length, here len = bucketNum * 2
        int arrayLen = bucketNum << 1;
        buffer.putInt(arrayLen);
        for(int i = 0; i < arrayLen; ++i){
            long longContent = bitSet[i];
            buffer.putLong(longContent);
        }
        buffer.flip();
        return buffer;
    }

    final int[] getSlotsByBucketNum(int bucketNum){
        int[] slots = new int[4];
        // a slot has 24 bits, then a bucket has 96 bits -> 1.5 long values
        int pos = (bucketNum * 3) >> 1;
        long value1 = bitSet[pos];
        long value2 = bitSet[pos + 1];
        if((bucketNum & 0x1) == 0){
            // even number: 2x
            slots[0] = (int) (value1 & 0xffffff);
            slots[1] = (int) ((value1 >> 24) & 0xffffff);
            slots[2] = (int) (((value1 >> 48) & 0xffff) + (value2 & 0xff));
            slots[3] = (int) ((value2 >> 8) & 0xffffff);
        }else{
            // odd number: 2x + 1
            slots[0] = (int) ((value1 >> 32) & 0xffffff);
            slots[1] = (int) (((value1 >> 56) & 0xff) + (value2 & 0xffff));
            slots[2] = (int) ((value2 >> 16) & 0xffffff);
            slots[3] = (int) ((value1 >> 40) & 0xffffff);
        }

        return slots;
    }

    boolean insertToBucket(int bucketIndex, long tag) {
        // here tag = fingerprint + interval_marker
        // below 3 code lines only used for debugging
        if((tag & 0xffff) == 0 || (tag > 0xffffff)){
            throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        }

        int fp = (int) (tag >> 16);
        int[] slots = getSlotsByBucketNum(bucketIndex);
        // a bucket has 4 slots
        int slotId = -1;
        for(int i = 0; i < 4; i++){
            int curFP = slots[i] >> 16;             // here we allow fp = 0
            int curInterval = slots[i] & 0xffff;
            if(curInterval == 0){
                // if curInterval = 0, then we think this is an empty slot
                slotId = i;
                break;
            }else if(curFP == fp){
                slotId = i;
                break;
            }
        }
        // if we can find tag, then coding should be 0,1,2,3 (bucketIndex is an even number); 5,6,7,8 (odd number)
        // otherwise, coding should be -1 and 4
        int coding = (bucketIndex & 0x1) * 5 + slotId;
        int writePos = (bucketIndex * 3) >> 1;
        long updateLongValue;
        switch (coding){
            case 0:
                updateLongValue = slots[0] | bitSet[writePos];
                bitSet[writePos] = updateLongValue;
                break;
            case 1:
                updateLongValue = ((long) slots[1] << 24) | bitSet[writePos];
                bitSet[writePos] = updateLongValue;
                break;
            case 2:
                updateLongValue = ((slots[2] & 0xffffL) << 48) | bitSet[writePos];
                bitSet[writePos] = updateLongValue;
                bitSet[writePos + 1] = (slots[2] & 0xff) | bitSet[writePos + 1];
                break;
            case 3:
                updateLongValue = ((long) slots[3] << 8) | bitSet[writePos + 1];
                bitSet[writePos + 1] = updateLongValue;
                break;
            case 5:
                // bucketIndex is odd (2x + 1), and slotId = 0
                updateLongValue = ((long) slots[0] << 32) | bitSet[writePos];
                bitSet[writePos] = updateLongValue;
                break;
            case 6:
                updateLongValue = ((slots[1] & 0xffL) << 56) | bitSet[writePos];
                bitSet[writePos] = updateLongValue;
                bitSet[writePos + 1] = (slots[1] >> 8) | bitSet[writePos + 1];
                break;
            case 7:
                updateLongValue = ((long) slots[2] << 16) | bitSet[writePos + 1];
                bitSet[writePos + 1] = updateLongValue;
                break;
            case 8:
                updateLongValue = ((long) slots[3] << 40) | bitSet[writePos + 1];
                bitSet[writePos + 1] = updateLongValue;
                break;
            default:
                return false;

        }
        return true;
    }

    boolean findTag(int i1, int i2, long tag) {
        long marker = tag & 0xffffL;
        if((tag & (tag - 1)) != 0 || (tag == 0)){
            throw new RuntimeException("marker should be the power of 2, current tag = " + marker);
        }
        int[] slots = getSlotsByBucketNum(i1);
        for(int slotContent : slots){
            if((slotContent & tag) == tag){
                return true;
            }
        }
        slots = getSlotsByBucketNum(i2);
        for(int slotContent : slots){
            if((slotContent & tag) == tag){
                return true;
            }
        }
        return false;
    }

    //return bucketIndex and slot position
    int[] findFingerprint(int i1, int i2, long fp) {
        long tag = fp << 16;
        int[] slots = getSlotsByBucketNum(i1);
        for(int slotPos = 0; slotPos < 4; slotPos++){
            // false write, should |
            boolean hold = fp == 0 ? ((slots[slotPos] & tag) < 0x10000) : ((slots[slotPos] & tag) == tag);
            if(hold){
                return new int[]{i1, slotPos};
            }
        }
        slots = getSlotsByBucketNum(i2);
        for(int slotPos = 0; slotPos < 4; slotPos++){
            boolean hold = fp == 0 ? ((slots[slotPos] & tag) < 0x10000) : ((slots[slotPos] & tag) == tag);
            if(hold){
                return new int[]{i2, slotPos};
            }
        }
        return new int[]{-1,-1};

    }

    int altIndex(int bucketIndex, long fingerprint) {
        return 0;
    }


    SFIndexAndFingerprint generate(long item) {
        return null;
    }


    long swapRandomTagInBucket(int bucketIndex, long tag) {
        return 0;
    }


    void updateTagInBucket(int i1, int i2, long tag) {

    }




    double getApproximateWindowNum() {
        // 8_16 -> 24 bits
        int count = 0;

        int bucketNum = getBucketNum();
        for(int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex = bucketIndex + 2){
            // even or odd
            // 0xffff00ffff00ffffL
            // 0xffff00L
        }

        return count / 16.0;
    }



    int rebuildTable(long[] hit_markers) {


        return 0;
    }


    long[][] compact() {
        return new long[0][];
    }


    void displayWithDecimal() {
        int bucketNum = getBucketNum();
        for(int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++){
            System.out.print(bucketIndex + "-th bucket:");
            int[] slots = getSlotsByBucketNum(bucketIndex);
            System.out.print(" (" + (slots[0] >> 16) + ", " + (slots[0] & 0xffff) +")");
            System.out.print(" (" + (slots[1] >> 16) + ", " + (slots[1] & 0xffff) +")");
            System.out.print(" (" + (slots[2] >> 16) + ", " + (slots[2] & 0xffff) +")");
            System.out.print(" (" + (slots[3] >> 16) + ", " + (slots[3] & 0xffff) +")");
            System.out.println();
        }
    }


    void displayWithHex() {
        int bucketNum = getBucketNum();
        for(int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++){
            System.out.print(bucketIndex + "-th bucket:");
            int[] slots = getSlotsByBucketNum(bucketIndex);
            System.out.print(" 0x" + Long.toHexString(slots[0]));
            System.out.print(" 0x" + Long.toHexString(slots[1]));
            System.out.print(" 0x" + Long.toHexString(slots[2]));
            System.out.print(" 0x" + Long.toHexString(slots[3]));
            System.out.println();
        }
    }

    SFTable8_16 copy() {
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable8_16(copyBitSet);
    }
}

```
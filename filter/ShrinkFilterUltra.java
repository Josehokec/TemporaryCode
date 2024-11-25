package filter;

import hasher.QuickHash;

import java.nio.ByteBuffer;

/**
 * advantages compared with shrink filter:
 * (1) same space, lower false positive rate;
 * (2) low transmission because we can compress updated markers
 * (3) do not need complex plan, only using greedy can obtain the lowest cost
 * disadvantages:
 * (1) high update cost
 * (2) we need to compress updated markers
 */
public class ShrinkFilterUltra {
    // note that original paper MAX_KICK_OUT = 500, due to we swap choose 4 position to kick,
    // to achieve a high load factor, we enlarge MAX_KICK_OUT to 550
    static final int MAX_KICK_OUT = 550;
    private static final double LOAD_FACTOR = 0.955;
    private SFTable12_20 table;
    private int bucketNum;

    // ptr: 0, 1, 2, 3, ..., 19
    private static final int[] leftIntervalMarkerArray = {
            0xfffff, 0xffffe, 0xffffc, 0xffff8, 0xffff0, 0xfffe0, 0xfffc0, 0xfff80,
            0xfff00, 0xffe00, 0xffc00, 0xff800, 0xff000, 0xfe000, 0xfc000, 0xf8000,
            0xf0000, 0xe0000, 0xc0000, 0x80000,
    };

    // ptr: 0, 1, 2, 3, ..., 19
    private static final int[] rightIntervalMarkerArray = {
            0x00001, 0x00003, 0x00007, 0x0000f, 0x0001f, 0x0003f, 0x0007f, 0x000ff,
            0x001ff, 0x003ff, 0x007ff, 0x00fff, 0x01fff, 0x03fff, 0x07fff, 0x0ffff,
            0x1ffff, 0x3ffff, 0x7ffff, 0xfffff,
    };

    // ptr: 0, 1, 2, 3, ..., 19
    private static final int[] tags = {
            0x00001, 0x00002, 0x00004, 0x00008, 0x00010, 0x00020, 0x00040, 0x00080,
            0x00100, 0x00200, 0x00400, 0x00800, 0x01000, 0x02000, 0x04000, 0x08000,
            0x10000, 0x20000, 0x40000, 0x80000
    };

    private ShrinkFilterUltra(SFTable12_20 table, int bucketNum) {
        this.table = table;
        this.bucketNum = bucketNum;
    }

    public static class Builder {
        private final long maxKeys;
        private SFTable12_20 table;

        //private int bitConfig;
        public Builder(long maxKeys) {
            this.maxKeys = maxKeys;
            table = null;
        }

        public Builder(SFTable12_20 table) {
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
     * before inserting a time interval, we first find whether tag has exists,
     * if yes, then we directly modify markers; otherwise, we insert it into a vacant slot
     * @param startTs - start timestamp
     * @param endTs - end timestamp
     * @param window - query window
     * @return - true if insert successfully
     */
    public boolean insert(long startTs, long endTs, long window) {
        long key = startTs / window;
        long[] intervalMarkers = getIntervalMarkers(startTs, endTs, window);
        int len = intervalMarkers.length;

        int fpLen = 12;
        int rightShift = 32;
        for (int i = 0; i < len; i++) {
            long curKey = key + i;
            long hashCode = QuickHash.hash64(curKey);
            int bucketIndex = (int) ((hashCode >> rightShift) & (bucketNum - 1));
            long fp = hashCode >>> (64 - fpLen);  // you can change this
            int altBucketIndex = altIndex(bucketIndex, fp);
            long tag = (fp << 20) | intervalMarkers[i];
            // debug code lines
            //System.out.println("key: " + curKey + " hashCode: " + Long.toHexString(hashCode) +
            //        " bucketIndex: " + bucketIndex + " altBucketIndex: " + altBucketIndex + " tag: " + Long.toHexString(tag));

            // due to we may insert same window id, to ensure same window id will merge,
            // we have to first seek the position with same fingerprint
            int[] bucketIdxAndSlotIdx = table.findFingerprint(bucketIndex, altBucketIndex, fp);
            if (bucketIdxAndSlotIdx[0] == -1) {
                if (!put(bucketIndex, altBucketIndex, tag)) {
                    return false;
                }
            } else {
                if (!putStar(bucketIdxAndSlotIdx[0], bucketIdxAndSlotIdx[1], tag)) {
                    return false;
                }
            }
        }
        return true;
    }

    private int altIndex(int bucketIndex, long fingerprint) {
        long altIndex = bucketIndex ^ ((fingerprint + 1) * 0xc4ceb9fe1a85ec53L);
        // now pull into valid range
        return (int) (altIndex & (bucketNum - 1));
    }

    public static int getLeftIntervalMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        // due to we default use 16 bits as interval marker and hit marker to quickly decode
        int ptr = (int) (distance * 20.0 / window);
        return leftIntervalMarkerArray[ptr];
    }

    public static int getRightIntervalMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 20.0 / window);
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
            ans[cnt++] = 0xfffff;       // you can change this
        }
        // third part
        ans[cnt] = getRightIntervalMarker(endTs, endWindowId * window, window);
        return ans;
    }

    public static long[][] getWindowIdAndMarkers(long startTs, long endTs, long window){
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;
        long offset = startWindowId * window;
        // in fact, endTs >= startTs + window
        // thus the first if condition (endWindowId == startWindowId) can be removed
        // here we use this condition (endWindowId == startWindowId) only for testing

        if (endWindowId == startWindowId) {
            int intervalMarker = getLeftIntervalMarker(startTs, offset, window) &
                    getRightIntervalMarker(endTs, offset, window);

            return new long[][]{{startWindowId, intervalMarker}};
        }

        // we split the range into three parts:
        // startWindowId, [startWindowId + 1, endWindowId - 1], endWindowId

        // first part
        long[][] ans = new long[(int) (endWindowId - startWindowId + 1)][2];
        int cnt = 0;
        ans[cnt++] = new long[]{startWindowId, getLeftIntervalMarker(startTs, offset, window)};

        // second part
        for (long key = startWindowId + 1; key < endWindowId; key++) {
            ans[cnt++] = new long[]{key, 0xfffffL}; // you can change this
        }
        // third part
        ans[cnt] = new long[]{endWindowId, getRightIntervalMarker(endTs, endWindowId * window, window)};
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

    // if we have find same fingerprint, then we directly insert
    private boolean putStar(int bucketIndex, int slotIndex, long tag) {
        return table.insertToBucket(bucketIndex, slotIndex, tag);
    }

    private boolean randomSwap(SFVictim victim) {
        // always choose alt bucket index
        int bucketIndex = victim.getAltBucketIndex();
        long replacedTag = table.swapRandomTagInBucket(bucketIndex, victim.getTag());
        int altBucketIndex = altIndex(bucketIndex, replacedTag >> 20);

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
        long hashCode = QuickHash.hash64(windowId);

        // if you want to change length of fingerprint and markers
        // please modify below two lines
        int fpLen = 12;
        long fp = hashCode >>> (64 - fpLen);
        int rightShift = 32;
        int bucketIndex = (int) ((hashCode >> rightShift) & (bucketNum - 1));
        int altBucketIndex = altIndex(bucketIndex, fp);
        // interval_marker occupies 20 bits
        long distance = ts - windowId * window;
        int ptr = (int) (distance * 20.0 / window);
        long tag = tags[ptr] | (fp << 20);
        return table.findTag(bucketIndex, altBucketIndex, tag);
    }

    // before updating range, we call this function to check window id
    // if return {-1,-1}, then we do not update
    // return bucket position and slot position
    public int[] queryWindowId(long windowId) {
        // cost 100ns
        long hashCode = QuickHash.hash64(windowId);

        // if you want to change length of fingerprint and markers
        // please modify below two lines and
        int fpLen = 12;
        long fp = hashCode >>> (64 - fpLen);
        // use high 32 bits as bucket index
        int rightShift = 32;
        int bucketIndex = (int) ((hashCode >> rightShift) & (bucketNum - 1));

        int altBucketIndex = altIndex(bucketIndex, fp);
        return table.findFingerprint(bucketIndex, altBucketIndex, fp);
    }

    public double getApproximateWindowNum() {
        return table.getApproximateWindowNum();
    }

    public void rebuild(UpdatedMarkers updatedMarkers) {
        int keyNum = table.updateMarkers(updatedMarkers);
        // here we support compact operation
        while (keyNum / (bucketNum * 4.0) < ShrinkFilterUltra.LOAD_FACTOR * 0.5 && bucketNum >= 16) {
            System.out.println("start compact shrink filter ultra...");
            // this function aims to rollback
            SFTable12_20 copiedTable = table.copy();
            bucketNum = bucketNum >> 1;
            // firstly, we get the contents of the second half of the bucket
            // high 32 bits in long value is fingerprint, low 32 bits are tag
            long[][] semi_bucket_fp_tags = table.getSemiBucketData();
            // then we try inset these tags, if we cannot insert successfully, we need rollback
            for (int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex) {
                for (int slotId = 0; slotId < 4; ++slotId) {
                    long fp_tag = semi_bucket_fp_tags[bucketIndex][slotId];
                    long fp = fp_tag >> 32;         // (fp_tag >> 32) & 0xfffL)
                    int altBucketIndex = altIndex(bucketIndex, fp);
                    if (fp_tag != 0) {
                        // we always ensure same fingerprint only exist once
                        int[] bucketIndexAndSlotIndex = table.findFingerprint(bucketIndex, altBucketIndex, fp);
                        if (bucketIndexAndSlotIndex[0] == -1) {
                            if (!put(bucketIndex, altBucketIndex, fp_tag & 0xffffffffL)) {
                                // cannot compact, we need to rollback
                                table = copiedTable;
                                bucketNum = bucketNum << 1;
                                // compactFlag = false
                                return;
                            }
                        } else {
                            if (!putStar(bucketIndexAndSlotIndex[0], bucketIndexAndSlotIndex[1], fp_tag & 0xffffffffL)) {
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
    }

    public void display(){
        table.displayWithHex();
    }

    public ByteBuffer serialize(){
        return table.serialize();
    }

    public static ShrinkFilterUltra deserialize(ByteBuffer buffer) {
        // ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int arrayLen = buffer.getInt();
        long[] bitSet = new long[arrayLen];
        // we cannot call this function: buffer.get(byte[] ...)
        for (int i = 0; i < arrayLen; ++i) {
            long bucketContent = buffer.getLong();
            bitSet[i] = bucketContent;
        }
        return new ShrinkFilterUltra.Builder(SFTable12_20.createTable(bitSet)).build();
    }

}


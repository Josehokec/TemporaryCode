package filter;


import hasher.XXHash;
import utils.BasicTypeToByteUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shrink Filter Table (SFT)
 * the tag (entry) of SFT is:
 *     fingerprint (8 bits) + interval_marker (4 bits) + hit_marker (4 bits)
 * bucket size of SFT is 4,which means a bucket of SFT occupies 64 bits
 * bucket storage format: |entry3|entry2|entry1|entry0|
 */
public class SFTable8_4_4 extends AbstractSFTable {
    private long[] bitSet;

    private SFTable8_4_4(int bucketNum) {
        bitSet = new long[bucketNum];
    }

    private SFTable8_4_4(long[] bitSet) {
        this.bitSet = bitSet;
    }

    // use builder to new SFTable
    static SFTable8_4_4 create(int bucketNum){
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new RuntimeException("Number of buckets (" + bucketNum + ") must be a power of two");
        }
        return new SFTable8_4_4(bucketNum);
    }

    static SFTable8_4_4 create(long[] bitSet){
        return new SFTable8_4_4(bitSet);
    }

    @Override
    int getBucketNum() {
        return bitSet.length;
    }

    @Override
    int getBucketByteSize(){
        return 8;
    }

    @Override
    long getLongArrayContent(int arrayPos) {
        return bitSet[arrayPos];
    }

    // no abstract method
    long getBucketContent(int bucketIndex){
        return bitSet[bucketIndex];
    }

    @Override
    ByteBuffer serialize(){
        // we only need to store table and bucketNum
        // if we support more table, we need to store table version
        int bucketNum = getBucketNum();
        int bucketSumSize = bucketNum * getBucketByteSize();
        ByteBuffer buffer = ByteBuffer.allocate(bucketSumSize + 4);

        // first write long array length, here bucketNum = len
        buffer.putInt(bucketNum);
        for(int i = 0; i < bucketNum; ++i){
            long bucketContent = bitSet[i];
            buffer.putLong(bucketContent);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * note that:
     *   1. if two item have same key, then we will merge them
     *   2. if two item have different key, but their hash value is same, we also merge them
     * insert tag, note that fingerprint can be zero, interval_marker cannot be zero
     * @param bucketIndex - bucketIndex
     * @param tag - (fingerprint, interval_marker, hit_marker)
     */
    @Override
    boolean insertToBucket(int bucketIndex, long tag){
        // debug, only 5~16 bits can be non-zero
        if((tag & 0xffffffffffff000fL) != 0){
            throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        }

        long fingerprint = tag >> 8;
        long bucketContent = bitSet[bucketIndex];
        // check four slot
        for(int slotPos = 0; slotPos < 4; ++slotPos){
            // move 16 bits
            int shift = slotPos << 4;
            long slotContent = (bucketContent >>> shift) & 0xffff;
            // System.out.println("slot content: " + Utils.tagToBinary((int) slotContent));
            // if this slot is vacant slot, then we directly insert tag
            if((slotContent & 0x00f0) == 0){
                long clearContent = (~(0xffffL << shift) & bucketContent);
                bitSet[bucketIndex] = (clearContent | (tag << shift));
                return true;
            }else if(fingerprint == (slotContent >> 8)){
                // if this slot has same fingerprint, then we directly update interval_marker
                bitSet[bucketIndex] = (bucketContent | (tag << shift));
                return true;
            }
        }
        // if this bucket without the same fingerprint or vacant slot, insert tag fails
        return false;
    }

    /**
     * here interval_marker only one bit with a value of 1, hit_marker = 0
     * @param i1 - bucket index 1
     * @param i2 - bucket index 2
     * @param tag - (fingerprint, interval_marker in {1,2,4,8}, hit_marker = 0)
     * @return - fingerprint exists and interval_marker & corresponding position not zero
     */
    @Override
    boolean findTag(int i1, int i2, long tag){
        // debug, only 5~16 bits can be non-zero
        //if((tag & 0xffffffffffff000fL) != 0 || ((tag & 0x0f0L) == 0)){
        //    throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        //}

        long bucketContent1 = bitSet[i1];
        long bucketContent2 = bitSet[i2];

        long marker = tag & 0x00f0;
        long mask = (marker << 48) + (marker << 32) + (marker << 16) + marker + 0xff00ff00ff00ff00L;
        // O(2) complexity, keep one bit in interval_marker
        return hasValue16Bits(bucketContent1 & mask, tag) || hasValue16Bits(bucketContent2 & mask, tag);
    }

    @Override
    boolean findFingerprint(int i1, int i2, long fp) {
        long bucketContent1 = bitSet[i1];
        long bucketContent2 = bitSet[i2];
        long value = fp << 8;
        long mask = 0xff00_ff00_ff00_ff00L;
        return hasValue16Bits(bucketContent1 & mask, value) || hasValue16Bits(bucketContent2 & mask, value);
    }

    private static long hasZero16Bits(long x){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (((x)-0x0001_0001_0001_0001L) & (~(x)) & 0x8000_8000_8000_8000L);
    }

    private static boolean hasValue16Bits(long x, long n){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (hasZero16Bits((x) ^ (0x0001_0001_0001_0001L * (n)))) != 0;
    }

    @Override
    int altIndex(int bucketIndex, long fingerprint) {
        // 0xc4ceb9fe1a85ec53L hash mixing constant from MurmurHash3
        // Similar value used in: https://github.com/efficient/cuckoofilter/
        // due to fingerprint can be zero, so we add one
        long altIndex = bucketIndex ^ ((fingerprint + 1) * 0xc4ceb9fe1a85ec53L);
        // now pull into valid range
        return hashIndex(altIndex);
    }

    @Override
    SFIndexAndFingerprint generate(long item){
        long hashCode = XXHash.hash64(BasicTypeToByteUtils.longToBytes(item), 8, 0);
        // low 8 bits as fingerprint
        int fingerprint = (int) (hashCode & 0xff);
        int bucketIndex = hashIndex(hashCode >> 8);
        return new SFIndexAndFingerprint(bucketIndex, fingerprint);
    }

    @Override
    long swapRandomTagInBucket(int bucketIndex, long tag){
        // generate random position from {0, 1, 2, 3}
        int randomSlotPosition = ThreadLocalRandom.current().nextInt(4);
        // move 16 bits
        int shift = randomSlotPosition << 4;
        long bucketContent = bitSet[bucketIndex];
        long returnTag = (int) ((bucketContent >>> shift) & 0xffff);
        long mask = ~(0x0ffffL << shift);
        // clear and set
        bitSet[bucketIndex] = ((bucketContent & mask) | (tag << shift));
        return returnTag;
    }

    /**
     * during update operation, we cannot insert any new tags
     * note that call this function you should ensure this tag indeed exists
     * note that hit_marker is 0000, interval marker may have multiple '1'
     * this function will change hit_marker
     * update operation is slower than query operation
     * @param i1 - bucket index
     * @param i2 - alternative bucket index
     * @param tag - (fingerprint, interval_marker != 0, hit_marker=0)
     */
    @Override
    void updateTagInBucket(int i1, int i2, long tag){
        // here hit_marker is 0000, interval marker may have multiple '1'
        if((tag & 0xffffffffffff000fL) != 0){
            throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        }

        long fingerprint = tag >> 8;
        long intervalMarker = (tag >> 4) & 0x0f;

        for(int bucketIndex : new int[]{i1, i2}) {
            long bucketContent = bitSet[bucketIndex];
            for (int slotPos = 0; slotPos < 4; ++slotPos) {
                // move 16 bits
                int shift = slotPos << 4;
                long slotContent = (bucketContent >>> shift) & 0xffff;
                long readIntervalMarker = (slotContent >> 4) & 0x0f;
                // when having same fingerprint, and read_interval_marker != 0
                // because fingerprint can be zero
                if ((slotContent >> 8) == fingerprint && readIntervalMarker != 0) {
                    long andIntervalMarker = readIntervalMarker & intervalMarker;
                    // then, we set hit_marker
                    if (andIntervalMarker != 0) {
                        // interval_marker -> hit_marker, update hit_marker
                        bucketContent |= (intervalMarker << shift);
                        bitSet[bucketIndex] = bucketContent;
                    }
                }
            }
        }
    }

    // no abstract method
    int hashIndex(long originIndex) {
        // we always need to return a bucket index within table range
        // we can return low bit because numBuckets is a pow of two
        return (int) (originIndex & (bitSet.length - 1));
    }

    /**
     * merge table, if hit_marker == 0, then we can discard this tag
     * @param xTable - another table
     */
    @Override
    void merge(AbstractSFTable xTable){
        int bucketNum = bitSet.length;
        if(bucketNum != xTable.getBucketNum()){
            throw new RuntimeException("two tables cannot merge, because their bucket number is different, " +
                    "size of this table is " + bucketNum + ", however, size of another table is " + xTable.getBucketNum());
        }
        for(int i = 0; i < bucketNum; ++i){
            bitSet[i] = bitSet[i] | xTable.getLongArrayContent(i);
        }
        // return this;
    }

    @Override
    double getApproximateWindowNum() {
        int count = 0;
        long mask = 0x00f0_00f0_00f0_00f0L;
        // for each bucket, we count the number of 1 in interval markers
        for (long bucketContent : bitSet) {
            count += Long.bitCount(bucketContent & mask);
        }
        return count / 4.0;
    }

    // if a time interval do not be hit, then we can remove this time interval
    // note that we ensure that tags are store in contiguous
    // this function will return the number of tags
    @Override
    int rebuildTable(){
        int tagNum = 0;
        int bucketNum = bitSet.length;
        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
            long bucketContent = bitSet[bucketIndex];
            int cursor = 0;
            long updateBucketContent = 0;
            for(int slotPos = 0; slotPos < 4; ++slotPos){
                long slotContent = bucketContent & 0xffff;
                long intervalMarker = (slotContent >> 4) & 0x0f;
                if(intervalMarker != 0){
                    // interval_marker & hit_maker (quick version)
                    long newIntervalMaker = intervalMarker & slotContent;
                    if(newIntervalMaker != 0){
                        // update interval_marker, and reset hit_maker = 0
                        long newSlotContent = (slotContent & 0xff00L) | (newIntervalMaker << 4);
                        updateBucketContent += (newSlotContent << cursor);
                        cursor += 16;
                        tagNum++;
                    }
                }
                bucketContent = bucketContent >> 16;
            }
            bitSet[bucketIndex] = updateBucketContent;
        }

        return tagNum;
    }

    // when load factor lower than LOAD_FACTOR * 0.5, we can choose to compress table
    // return <fp,tag> from second semi-bucket, we need tag length <= 32
    @Override
    long[][] compact(){
        int originalBucketNum = getBucketNum();
        int newBucketNum = originalBucketNum >> 1;

        // high 32 bits: fingerprint, low 32 bits: tag
        long[][] fp_tags = new long[newBucketNum][4];
        for(int i = newBucketNum; i < originalBucketNum; ++i){
            long bucketContent = bitSet[i];
            if(bucketContent != 0){
                for(int slotId =0; slotId < 4; ++slotId){
                    long tag = bucketContent & 0xffff;
                    // note that high 32 bits are fp, low 32 bits are tag
                    fp_tags[i - newBucketNum][slotId] = tag + ((tag >> 8) << 32);
                    bucketContent = (bucketContent >> 16);
                }
            }
        }
        //compact
        long[] newBitSet = new long[newBucketNum];
        System.arraycopy(bitSet, 0, newBitSet, 0, newBucketNum);
        bitSet = newBitSet;
        return fp_tags;
    }

    // this function is used for debugging
    @Override
    void displayWithDecimal(){
        int len = bitSet.length;
        for(int bucketIndex = 0; bucketIndex < len; ++bucketIndex){
            long bucketContent = bitSet[bucketIndex];
            System.out.print(bucketIndex + "-th bucket:");
            for(int pos = 0; pos < 4; ++pos){
                int shift = pos << 4;
                long slotContent = (bucketContent >> shift) & 0xffff;
                long hitMarker = slotContent & 0x0f;
                long intervalMaker = (slotContent >>> 4) & 0x0f;
                long fingerprint = (slotContent >>> 8) & 0xff;
                System.out.print(" (" + fingerprint + "," + intervalMaker + "," + hitMarker + ")");
            }
            //System.out.print(" -> binary: " + Utils.longToBinary(bucketContent));
            System.out.println();
        }
    }

    // this function is used for debugging
    @Override
    void displayWithHex(){
        // debug
        int len = bitSet.length;
        for(int buckeIndex = 0; buckeIndex < len; ++buckeIndex){
            long bucketContent = bitSet[buckeIndex];
            System.out.print(buckeIndex + "-th bucket:");
            for(int pos = 0; pos < 4; ++pos){
                int shift = pos << 4;
                long slotContent = (bucketContent >> shift) & 0xffff;
                System.out.print(" 0x" + Long.toHexString(slotContent));
            }
            System.out.println();
        }
    }

    @Override
    AbstractSFTable copy(){
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable8_4_4(copyBitSet);
    }
}

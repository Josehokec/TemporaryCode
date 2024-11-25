package filter;

import hasher.XXHash;
import utils.BasicTypeToByteUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shrink Filter Table (SFT)
 * the tag (entry) of SFT is:
 *     fingerprint (8 bits) + interval_marker (12 bits) + hit_marker (12 bits)
 * bucket size of SFT is 4,which means a bucket of SFT occupies 128 bits
 * bucket storage format:    |entry1|entry0|
 *                           |entry3|entry2|
 */
public class SFTable8_12_12 extends AbstractSFTable {
    private long[] bitSet;

    private SFTable8_12_12(int bucketNum){
        // an entry occupies 32 bits, then a bucket occupies 128 bit
        // thus, we need $[bucketNum * 2] long numbers
        bitSet = new long[bucketNum << 1];
    }

    private SFTable8_12_12(long[] bitSet) {
        this.bitSet = bitSet;
    }

    static SFTable8_12_12 create(int bucketNum) {
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new IllegalArgumentException("bucketNum must be a power of 2");
        }
        return new SFTable8_12_12(bucketNum);
    }

    static SFTable8_12_12 create(long[] bitSet) {
        return new SFTable8_12_12(bitSet);
    }

    @Override
    int getBucketNum(){
        return bitSet.length >> 1;
    }

    @Override
    int getBucketByteSize(){
        return 16;
    }

    // no abstract method
    long[] getBucketContent(int bucketIndex){
        int arrayPos = bucketIndex << 1;
        return new long[]{bitSet[arrayPos], bitSet[arrayPos + 1]};
    }

    @Override
    long getLongArrayContent(int arrayPos){
        return bitSet[arrayPos];
    }

    // no abstract method
    long[] getBucketContent(int bucketIndex1, int bucketIndex2){
        int arrayPos1 = bucketIndex1 << 1;
        int arrayPos2 = bucketIndex2 << 1;
        return new long[]{bitSet[arrayPos1], bitSet[arrayPos1+1], bitSet[arrayPos2], bitSet[arrayPos2+1]};
    }

    @Override
    ByteBuffer serialize(){
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
        // 1-st ~ 12-th bits are hit_marker, which should be zero
        if((tag & 0xffffffff_00_000_fffL) != 0){
            throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        }

        long fingerprint = tag >> 24;
        long[] bucketContent = getBucketContent(bucketIndex);
        for(int i = 0; i < 2; ++i){
            long longNum = bucketContent[i];
            // write position
            int writePos = (bucketIndex << 1) + i;
            // if first interval_marker = 0, then insert it directly
            // or if first fingerprint is same, then insert it directly
            boolean insertFirstPos = ((longNum & 0x0_fff_000L) == 0) | (((longNum >> 24) & 0xff) == fingerprint);

            if(insertFirstPos){
                bitSet[writePos] |= tag;
                return true;
            }else {
                boolean insertSecondPos = ((longNum & 0x0_fff_000_00_000_000L) == 0) |
                        (((longNum >> 56) & 0xff) == fingerprint);
                if(insertSecondPos){
                    bitSet[writePos] |= (tag << 32);
                    return true;
                }
            }
        }
        // if this bucket without the same fingerprint or vacant slot, insert tag fails
        return false;
    }

    /**
     * here interval_marker only one bit with a value of 1, hit_marker = 0
     * @param i1 - bucket index 1
     * @param i2 - bucket index 2
     * @param tag - (fingerprint, interval_marker, hit_marker = 0)
     * @return - fingerprint exists and interval_marker & corresponding position not zero
     */
    @Override
    boolean findTag(int i1, int i2, long tag) {
        // last 12 bits should be 0, and 12~24bits cannot be 0
        if ((tag & 0xffffffff_00_000_fffL) != 0 || ((tag & 0x0_fff_000L) == 0)) {
            throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        }

        long[] bucketContent = getBucketContent(i1, i2);
        long marker = tag & 0xfff_000L;
        long mask = (marker << 32) + marker + 0xff_000_000_ff_000_000L;
        return hasValue32Bits(bucketContent[0] & mask, tag) || hasValue32Bits(bucketContent[1] & mask, tag) ||
                hasValue32Bits(bucketContent[2] & mask, tag) || hasValue32Bits(bucketContent[3] & mask, tag);
    }

    @Override
    boolean findFingerprint(int i1, int i2, long fp) {
        long[] bucketContent = getBucketContent(i1, i2);
        long value = fp << 24;
        long mask = 0xff_000000_ff_000000L;
        return hasValue32Bits(bucketContent[0] & mask, value) || hasValue32Bits(bucketContent[1] & mask, value) ||
                hasValue32Bits(bucketContent[2] & mask, value) || hasValue32Bits(bucketContent[3] & mask, value);
    }

    private static long hasZero32Bits(long x){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (((x)-0x0000_0001_0000_0001L) & (~(x)) & 0x8000_0000_8000_0000L);
    }

    private static boolean hasValue32Bits(long x, long n){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (hasZero32Bits((x) ^ (0x0000_0001_0000_0001L * (n)))) != 0;
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
        int fingerprint = (int) (hashCode & 0x00ff);
        int bucketIndex = hashIndex(hashCode >> 8);
        return new SFIndexAndFingerprint(bucketIndex, fingerprint);
    }

    @Override
    long swapRandomTagInBucket(int bucketIndex, long tag){
        // generate random position from {0, 1, 2, 3}
        int randomSlotPosition = ThreadLocalRandom.current().nextInt(4);
        long returnTag;
        int writePos;
        switch(randomSlotPosition){
            case 0:
                writePos = bucketIndex << 1;
                returnTag = bitSet[writePos] & 0x0_ffff_ffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffff_ffff_0000_0000L) | tag;
                break;
            case 1:
                writePos = bucketIndex << 1;
                returnTag = (bitSet[writePos] >> 32)  & 0x0_ffff_ffffL;
                bitSet[writePos] = (bitSet[writePos] & 0x0000_0000_ffff_ffffL) | (tag << 32);
                break;
            case 2:
                writePos = (bucketIndex << 1) + 1;
                returnTag = bitSet[writePos] & 0x0_ffff_ffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffff_ffff_0000_0000L) | tag;
                break;
            default:
                // case 3
                writePos = (bucketIndex << 1) + 1;
                returnTag = (bitSet[writePos] >> 32) & 0x0_ffff_ffffL;
                bitSet[writePos] = (bitSet[writePos] & 0x0000_0000_ffff_ffffL) | (tag << 32);
        }
        //System.out.println("returnTag ==> 0x" + Long.toHexString(returnTag));
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
    void updateTagInBucket(int i1, int i2, long tag) {
        // here hit_marker is 0000, interval marker may have multiple '1'
        if((tag & 0xffff_ffff_00_000_fffL) != 0){
            throw new RuntimeException("tag (" + BasicTypeToByteUtils.longToBinary(tag) + ") is illegal.");
        }

        long fingerprint = tag >> 24;
        long intervalMarker = (tag >> 12) & 0x0fff;

        long[] longNums = getBucketContent(i1, i2);
        for(int i = 0; i < 4; ++i){
            long longNum = longNums[i];
            long lowFingerprint = (longNum >> 24) & 0x0ff;
            long lowIntervalMarker = (longNum >> 12) & 0x0fff;
            if(lowFingerprint == fingerprint && lowIntervalMarker != 0){
                long andIntervalMarker = intervalMarker & lowIntervalMarker;
                if(andIntervalMarker != 0){
                    // interval_marker -> hit_marker, update hit_marker
                    longNum |= andIntervalMarker;
                    // update bitSet
                    int writePos = getWritePos(i, i1, i2);
                    bitSet[writePos] = longNum;
                }
            }else{
                long highFingerprint = (longNum >> 56) & 0x0ff;
                long highIntervalMarker = (longNum >> 44) & 0x0fff;
                if(highFingerprint == fingerprint && highIntervalMarker != 0){
                    long andIntervalMarker = intervalMarker & highIntervalMarker;
                    // interval_marker -> hit_marker, update hit_marker
                    longNum |= (andIntervalMarker << 32);
                    // update bitSet
                    int writePos = getWritePos(i, i1, i2);
                    bitSet[writePos] = longNum;
                }
            }
        }
    }

    static int getWritePos(int slotId, int bucketIndex1, int bucketIndex2){
        int writePos;
        switch (slotId){
            case 0:
                writePos = bucketIndex1 << 1;
                break;
            case 1:
                writePos = (bucketIndex1 << 1) + 1;
                break;
            case 2:
                writePos = bucketIndex2 << 1;
                break;
            default:
                writePos = (bucketIndex2 << 1) + 1;
        }
        return writePos;
    }

    int hashIndex(long originIndex) {
        // we always need to return a bucket index within table range
        // we can return low bit because numBuckets is a pow of two
        return (int) (originIndex & ((bitSet.length >> 1) - 1));
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
        // 8_12_12 -> 32 bits, a long value only has two slots
        int count = 0;
        long mask = 0x00fff000_00fff000L;
        // for each bucket, we count the number of 1 in interval markers
        for (long semiBucketContent : bitSet) {
            count += Long.bitCount(semiBucketContent & mask);
        }
        return count / 12.0;
    }

    // if a time interval do not be hit, then we can remove this time interval
    // this function will return the number of tags
    // 2024-20-28
    @Override
    int rebuildTable() {
        int tagNum = 0;
        int bucketNum = getBucketNum();

        long[] fourSlotContent = new long[4];
        for(int i = 0; i < bucketNum; ++i){
            int pos = i << 1;
            fourSlotContent[0] = bitSet[pos] & 0xffffffffL;
            fourSlotContent[1] = (bitSet[pos] >> 32) & 0xffffffffL;
            fourSlotContent[2] = bitSet[pos + 1] & 0xffffffffL;
            fourSlotContent[3] = (bitSet[pos + 1] >> 32) & 0xffffffffL;

            int cnt = 0;
            for(int slotId = 0; slotId < 4; ++slotId){
                // please modify
                long newIntervalMaker = ((fourSlotContent[slotId] >> 12) & 0xfff) & fourSlotContent[slotId];
                if(newIntervalMaker != 0){
                    // please modify
                    fourSlotContent[cnt] = (fourSlotContent[slotId] & 0xff_000_000L) | (newIntervalMaker << 12);
                    cnt++;
                }
            }

            tagNum += cnt;
            switch (cnt){
                case 0:
                    bitSet[pos] = 0;
                    bitSet[pos + 1] = 0;
                    break;
                case 1:
                    bitSet[pos] = fourSlotContent[0];
                    bitSet[pos + 1] = 0;
                    break;
                case 2:
                    bitSet[pos] = (fourSlotContent[1] << 32) | fourSlotContent[0];
                    bitSet[pos + 1] = 0;
                    break;
                case 3:
                    bitSet[pos] = (fourSlotContent[1] << 32) | fourSlotContent[0];
                    bitSet[pos + 1] = fourSlotContent[2];
                    break;
                case 4:
                    bitSet[pos] = (fourSlotContent[1] << 32) | fourSlotContent[0];
                    bitSet[pos + 1] = (fourSlotContent[3] << 32) | fourSlotContent[2];
                    break;
                default:
                    throw new RuntimeException("wrong state");
            }
        }

        return tagNum;
    }

    // when load factor lower than LOAD_FACTOR * 0.5, we can choose to compress table
    // return <fp,tag> from second semi-bucket, we need tag length <= 32
    @Override
    long[][] compact(){
        int longValueNum = bitSet.length;
        // high 32 bits: fingerprint, low 32 bits: tag
        int originalBucketNum = longValueNum >> 1;
        int newBucketNum = longValueNum >> 2;
        long[][] fp_tags = new long[newBucketNum][4];
        for(int i = newBucketNum; i < originalBucketNum; ++i){
            int bucketPos = i - newBucketNum;
            long[] values = getBucketContent(i);
            if(values[0] != 0){
                long firstTag = values[0] & 0xffffffffL;
                long secondTag = (values[0] >> 32) & 0xffffffffL;
                long thirdTag = values[1] & 0xffffffffL;
                long fourthTag = (values[1] >> 32) & 0xffffffffL;
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

    // this function is used for debugging
    @Override
    void displayWithDecimal(){
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
            System.out.print(bucketIndex + "-th bucket:");
            long[] twoLongNums = getBucketContent(bucketIndex);
            for(int i = 0; i < 2; ++i){
                long longNum = twoLongNums[i];
                long lowFingerprint = (longNum >> 24) & 0x0ff;
                long lowIntervalMarker = (longNum >> 12) & 0x0fff;
                long lowHitMarker = longNum & 0x0fff;
                System.out.print(" (" + lowFingerprint + "," + lowIntervalMarker + "," + lowHitMarker + ")");
                // ---
                long highFingerprint = (longNum >> 56) & 0x0ff;
                long highIntervalMarker = (longNum >> 44) & 0x0fff;
                long highHitMarker = (longNum >> 32) & 0x0fff;
                System.out.println(" (" + highFingerprint + "," + highIntervalMarker + "," + highHitMarker + ")");
            }
            System.out.println();
        }
    }

    // this function is used for debugging
    @Override
    void displayWithHex(){
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
            System.out.print(bucketIndex + "-th bucket:");
            long[] twoLongNums = getBucketContent(bucketIndex);
            long longNum1 = twoLongNums[0];
            long longNum2 = twoLongNums[1];
            System.out.print(" 0x" + Long.toHexString(longNum1 & 0x0_ffff_ffffL));
            System.out.print(" 0x" + Long.toHexString((longNum1 >> 32) & 0x0_ffff_ffffL));
            System.out.print(" 0x" + Long.toHexString(longNum2 & 0x0_ffff_ffffL));
            System.out.print(" 0x" + Long.toHexString((longNum2 >> 32) & 0x0_ffff_ffffL));
            System.out.println();
        }
    }

    @Override
    AbstractSFTable copy(){
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable8_12_12(copyBitSet);
    }
}

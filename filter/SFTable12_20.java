package filter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * assume that f = len(fingerprint), m = len(interval_marker)
 * and a bucket has 128 bits, then fpr = 8 / (2 ** f) + 1 / m
 * we can know that f = 12, m = 20, fpr has minimum value: 0.052
 */
public class SFTable12_20 {
    private long[] bitSet;

    private SFTable12_20(int bucketNum){
        // a bucket occupies 128 bits
        bitSet = new long[bucketNum << 1];
    }

    private SFTable12_20(long[] bitSet){
        this.bitSet = bitSet;
    }

    public static SFTable12_20 createTable(int bucketNum){
        // only used for debugging
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new IllegalArgumentException("bucketNum must be a power of 2, current bucketNum = " + bucketNum);
        }
        return new SFTable12_20(bucketNum);
    }

    public static SFTable12_20 createTable(long[] bitSet){
        return new SFTable12_20(bitSet);
    }

    public int getBucketNum() {
        return bitSet.length >> 1;
    }

    public int getBucketByteSize() {
        return 16;
    }

    public long getLongArrayContent(int arrayPos) {
        return bitSet[arrayPos];
    }

    public ByteBuffer serialize() {
        int bucketNum = getBucketNum();
        int bucketSumSize = bucketNum * getBucketByteSize();
        ByteBuffer buffer = ByteBuffer.allocate(bucketSumSize + 4);
        // a bucket has two long value
        buffer.putInt(bucketNum << 1);
        for(long val : bitSet){
            buffer.putLong(val);
        }
        buffer.flip();
        return buffer;
    }

    // you should first call findTag() function to get the bucket index
    // then call insertToBucket function
    public boolean insertToBucket(int bucketIndex, long tag) {
        // tag = <fingerprint, interval_marker>, below 3 code lines only used for debugging
        //if((tag & 0xfffff) == 0 || (tag > 0xffffffffL)){
        //    throw new RuntimeException("tag (" + Long.toBinaryString(tag) + ") is illegal.");
        //}
        int pos = bucketIndex << 1;
        // due to we have call findFingerprint function,
        // we know without same fingerprint, then we only
        // need to find a vacant slot (i.e., intervalMarker = 0)

        for(int idx : new int[]{pos, pos + 1}){
            long val = bitSet[idx];
            if((val & 0xfffff) == 0){
                bitSet[idx] |= tag;
                return true;
            }
            if(((val >> 32) & 0xfffff) == 0){
                bitSet[idx] |= (tag << 32);
                return true;
            }
        }
        return false;
    }

    public boolean insertToBucket(int bucketIndex, int slotPos, long tag){
        // slotPos: 0, 1, 2, 3
        switch (slotPos){
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

    public boolean findTag(int i1, int i2, long tag) {
        // last 8 bits should be 0, and 9~16bits cannot be 0
        //if (tag > 0xffffffffL || (((tag & 0xfffffL) & ((tag & 0xfffffL) - 1)) != 0)) {
        //    throw new RuntimeException("tag (" + Long.toBinaryString(tag) + ") is illegal.");
        //}

        long intervalMarker = tag & 0xfffffL;
        long mask = (intervalMarker << 32) | intervalMarker | 0xfff_00000_fff_00000L;

        int pos1 = i1 << 1;
        int pos2 = i2 << 1;
        return hasValue32Bits(bitSet[pos1] & mask, tag) || hasValue32Bits(bitSet[pos1 + 1] & mask, tag) ||
                hasValue32Bits(bitSet[pos2] & mask, tag) || hasValue32Bits(bitSet[pos2 + 1] & mask, tag);
    }

    private static long hasZero32Bits(long x){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (((x)-0x0000_0001_0000_0001L) & (~(x)) & 0x8000_0000_8000_0000L);
    }

    private static boolean hasValue32Bits(long x, long n){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (hasZero32Bits((x) ^ (0x0000_0001_0000_0001L * (n)))) != 0;
    }

    // public final int[] findFingerprint(int i1, int i2, long fp)
    public final int[] findFingerprint(int i1, int i2, long fp) {
        // call this function is time-consuming:, cost about 100 ns
        int[] idxes = {i1 << 1, (i1 << 1) | 0x1, i2 << 1, (i2 << 1) | 0x1};
        long mask = 0xffffffffL;
        long threshold = 0x100000;
        int shift20 = 20;
        int shift52 = 52;

        for (int i = 0; i < 4; i++) {
            int arrayPos = idxes[i];
            long v = bitSet[arrayPos];
            if (fp == 0) {
                long v1 = v & mask;
                long v2 = v >>> 32;
                if ((v1 < threshold && v1 > 0) || (v2 < threshold && v2 > 0)) {
                    int j = (v2 < threshold && v2 > 0) ? 1 : 0;
                    return new int[]{arrayPos >> 1, (((i << 1) | j)) & 0x3};
                }
            } else {
                if (((v >> shift20) & 0xfff) == fp || ((v >>> shift52) == fp)) {
                    int j = (v >>> shift52) == fp ? 1 : 0;
                    return new int[]{arrayPos >> 1, (((i << 1) | j)) & 0x3};
                }
            }
        }
        return new int[]{-1, -1};
    }

    long swapRandomTagInBucket(int bucketIndex, long tag) {
        // generate random position from {0, 1, 2, 3}, 3 has bug
        int randomSlotPosition = ThreadLocalRandom.current().nextInt(4);
        long returnTag;
        int writePos;
        //System.out.println("swap tag: " + Long.toHexString(tag) + " swap pos: " +  randomSlotPosition);
        switch(randomSlotPosition){
            case 0:
                writePos = bucketIndex << 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff00000000L) | tag;
                break;
            case 1:
                writePos = bucketIndex << 1;
                returnTag = (bitSet[writePos] >> 32)  & 0xffffffffL;
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

    public int updateMarkers(UpdatedMarkers updatedMarkers){
        int keyNum = 0;
        int len = bitSet.length;
        for(int pos = 0; pos < len; pos++){
            bitSet[pos] &= updatedMarkers.getMaskedLongValue(pos);
            if((bitSet[pos] & 0xfffff) == 0){
                bitSet[pos] &= 0xffffffff_00000000L;
            }else{
                keyNum++;
            }
            if((bitSet[pos] & 0xfffff_00000000L) == 0){
                bitSet[pos] &= 0x00000000_ffffffffL;
            }else{
                keyNum++;
            }

        }
        return keyNum;
    }

    public double getApproximateWindowNum() {
        int count = 0;
        for(long val : bitSet){
            count += Long.bitCount(val & 0x000fffff000fffffL);
        }
        return count / 20.0;
    }

    // before compacting we need to get
    public long[][] getSemiBucketData() {
        int longValueNum = bitSet.length;
        // high 32 bits: fingerprint, low 32 bits: tag
        int originalBucketNum = longValueNum >> 1;
        int newBucketNum = longValueNum >> 2;
        long[][] fp_tags = new long[newBucketNum][4];
        for(int i = newBucketNum; i < originalBucketNum; ++i){
            int bucketPos = i - newBucketNum;
            int pos = i << 1;
            long val1 = bitSet[pos];
            long val2 = bitSet[pos + 1];
            long firstTag = val1 & 0xffffffffL;
            long secondTag = val1 >>> 32;
            long thirdTag = val2 & 0xffffffffL;
            long fourthTag = val2 >>> 32;
            // please modify below four lines
            fp_tags[bucketPos][0] = firstTag | ((firstTag & 0xfff_00000L) << 12);
            fp_tags[bucketPos][1] = secondTag | ((secondTag & 0xfff_00000L) << 12);
            fp_tags[bucketPos][2] = thirdTag | ((thirdTag & 0xfff_00000L) << 12);
            fp_tags[bucketPos][3] = fourthTag | ((fourthTag & 0xfff_00000L) << 12);
        }
        // compact
        long[] newBitSet = new long[longValueNum >> 1];
        System.arraycopy(bitSet, 0, newBitSet, 0, longValueNum >> 1);
        bitSet = newBitSet;
        return fp_tags;
    }

    public void displayWithHex() {
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++){
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

    public SFTable12_20 copy() {
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable12_20(copyBitSet);
    }

    // 这里测试
    public void writeFile(String fileName){
        // longArray.data
        try (FileOutputStream fos = new FileOutputStream(fileName + ".dat");
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(bitSet);
            System.out.println("Long数组已成功写入文件。");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static SFTable12_20 read(String fileName) {
        long[] readBitSet = null;
        try (FileInputStream fis = new FileInputStream(fileName + ".dat");
             ObjectInputStream ois = new ObjectInputStream(fis)) {

            readBitSet = (long[]) ois.readObject();
//            System.out.println("从文件读取的Long数组：");
//            for (Long l : readBitSet) {
//                System.out.println(l);
//            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return SFTable12_20.createTable(readBitSet);
    }
}

/*
version 2:
//        int[] idxes = new int[]{i1 << 1, (i1 << 1) | 0x1, i2 << 1, (i2 << 1) | 0x1};
//        if(fp == 0){
//            for(int i = 0; i < 4; i++){
//                int arrayPos = idxes[i];
//                long v1 = bitSet[arrayPos] & 0xffffffffL;
//                long v2 = (bitSet[arrayPos] >> 32) & 0xffffffffL;
//                if((v1 < 0x100000 && v1 > 0) || (v2 < 0x100000 && v2 > 0)){
//                    int j = (v1 < 0x100000 && v1 > 0) ? 0 : 1;
//                    return new int[]{arrayPos >> 1, ((i << 1 | j)) & 0x3};
//                }
//            }
//        }else{
//            for(int i = 0; i < 4; i++){
//                int arrayPos = idxes[i];
//                long v = bitSet[arrayPos];
//                if(((v >> 20) & 0xfff) == fp || ((v >> 52) & 0xfff) == fp){
//                    int j = ((v >> 20) & 0xfff) == fp ? 0 : 1;
//                    return new int[]{arrayPos >> 1, ((i << 1 | j)) & 0x3};
//                }
//            }
//        }


// version 1
// something was wrong
//        for(int bucketIndex : new int[]{i1, i2}){
//            int arrayPos = bucketIndex << 1;
//            if(bitSet[arrayPos] != 0){
//                long fp0 = (bitSet[arrayPos] >> 20) & 0xfff;
//                long intervalMarker0 = bitSet[arrayPos] & 0xfffff;
//                long fp1 = (bitSet[arrayPos] >> 52) & 0xfff;
//                long intervalMarker1 = (bitSet[arrayPos] >> 32) & 0xfffff;
//                if(fp0 == fp && intervalMarker0 != 0){
//                    return new int[]{bucketIndex, 0};
//                }
//                if(fp1 == fp && intervalMarker1 != 0){
//                    return new int[]{bucketIndex, 1};
//                }
//
//                if(bitSet[arrayPos + 1] != 0){
//                    long fp2 = (bitSet[arrayPos + 1] >> 20) & 0xfff;
//                    long intervalMarker2 = bitSet[arrayPos + 1] & 0xfffff;
//                    long fp3 = (bitSet[arrayPos + 1] >> 52) & 0xfff;
//                    long intervalMarker3 = (bitSet[arrayPos + 1] >> 32) & 0xfffff;
//
//                    if(fp2 == fp && intervalMarker2 != 0){
//                        return new int[]{bucketIndex, 2};
//                    }
//                    if(fp3 == fp && intervalMarker3 != 0){
//                        return new int[]{bucketIndex, 3};
//                    }
//                }
//            }
//        }
 */
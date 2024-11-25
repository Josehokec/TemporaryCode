package filter;

import hasher.XXHash;
import utils.BasicTypeToByteUtils;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * lock free bloom filter
 * here we use double hashing techniques to generate k hash function values
 */
public class LockFreeBloomFilter{
    private static final long serialVersionUID = 2L;
    LockFreeBitArray bitArray;
    private final int k;                // number of hash functions
    public static int seed = 7;

//    public static long estimatedBitSize(int exceptedItemNum){
//        // default fpr -> 0.01
//        double fpr = 0.01;
//        return (long) Math.ceil(1.44 * exceptedItemNum * (-Math.log(fpr) / Math.log(2)));
//    }

    public LockFreeBloomFilter(int bitSize, int exceptedItemNum){
        // m = bitSize, n = exceptedItemNum
        bitArray = new LockFreeBitArray(bitSize);
        k = (int) Math.round(Math.log(2) * bitSize / exceptedItemNum);
    }

    public LockFreeBloomFilter(double fpr, int itemNum){
        // k = ceil(-log_2(fpr))
        double value = -(Math.log(fpr) / Math.log(2));
        k = (int) Math.ceil(value);
        long bitSize = (long) Math.ceil(1.44 * itemNum * value);
        bitArray = new LockFreeBitArray(bitSize);
    }

    public LockFreeBloomFilter(int k, LockFreeBitArray bitArray){
        this.k = k;
        this.bitArray = bitArray;
    }

    public void insert(String key){
        byte[] data = key.getBytes();
        /* slow version
        for(int i = 0; i < k; ++i){
            long hashValue = XXHash.hash64(data, data.length, seeds[i]);
            long position = hashValue & Long.MAX_VALUE;
            bitArray.set(position);
        }*/
        long hashValue = XXHash.hash64(data, data.length, seed);
        // double hashing techniques: h_i(key)=h_1(key)+i * h_2(key)
        int low32bit = (int) hashValue;
        int high32bit = (int) (hashValue >>> 32);
        long bitSize = bitArray.getBitNum();

        long combineHash = low32bit;
        for(int i = 0; i < k; ++i){
            // to avoid generate negative number, we need to use AND operator
            bitArray.set((combineHash & Long.MAX_VALUE) % bitSize);
            combineHash += high32bit;
        }
    }

    public boolean contains(String key){
        byte[] data = key.getBytes();
        long hashValue = XXHash.hash64(data, data.length, seed);
        int low32bit = (int) hashValue;
        int high32bit = (int) (hashValue >>> 32);
        long bitSize = bitArray.getBitNum();

        long combineHash = low32bit;
        for(int i = 0; i < k; ++i){
            if(!bitArray.get((combineHash & Long.MAX_VALUE) % bitSize)){
                return false;
            }
            combineHash += high32bit;
        }

        return true;
    }

    /**
     * v1.x = v2.x within w seconds
     * @param key - attribute value(s)
     * @param windowId - windowId = ts / query_window
     */
    public void insertWithWindow(String key, long windowId){
        // we use window as seed
        byte[] data = key.getBytes();
        long hashValue = XXHash.hash64(data, data.length, windowId);

        int low32bit = (int) hashValue;
        int high32bit = (int) (hashValue >>> 32);
        long bitSize = bitArray.getBitNum();

        long combineHash = low32bit;
        for(int i = 0; i < k; ++i){
            // to avoid generate negative number, we need to use AND operator
            bitArray.set((combineHash & Long.MAX_VALUE) % bitSize);
            combineHash += high32bit;
        }
    }

    public boolean containsWithWindow(String key, long windowId){
        byte[] data = key.getBytes();
        long hashValue = XXHash.hash64(data, data.length, windowId);


        int low32bit = (int) hashValue;
        int high32bit = (int) (hashValue >>> 32);
        long bitSize = bitArray.getBitNum();

        long combineHash = low32bit;
        for(int i = 0; i < k; ++i){
            if(!bitArray.get((combineHash & Long.MAX_VALUE) % bitSize)){
                return false;
            }
            combineHash += high32bit;
        }
        return true;
    }

    public ByteBuffer serialize(){
        // we store k, number of long value and long array
        ByteBuffer bitBuffer = bitArray.serialize();
        ByteBuffer res = ByteBuffer.allocate(bitBuffer.capacity() + 8);
        res.putInt(k);
        res.putInt(bitArray.bitArray.length());
        res.put(bitBuffer);
        res.flip();
        return res;
    }

    public static LockFreeBloomFilter deserialize(ByteBuffer buffer) {
        // here we modify capacity() function to remaining() function
        int k = buffer.getInt();
        int longValueNum  = buffer.getInt();
        long[] longValues = new long[longValueNum];
        for(int i = 0; i < longValues.length; ++i){
            longValues[i] = buffer.getLong();
        }
        LockFreeBitArray array = new LockFreeBitArray(longValues);
        return new LockFreeBloomFilter(k, array);
    }
}

/*
discard code:
byte[] data = key.getBytes();
byte[] w = BasicTypeToByteUtils.longToBytes(windowId);
byte[] newKey = new byte[data.length + w.length];
System.arraycopy(w, 0, newKey, 0, w.length);
System.arraycopy(data, 0, newKey, w.length, data.length);
long hashValue = XXHash.hash64(newKey, data.length, seed);
 */
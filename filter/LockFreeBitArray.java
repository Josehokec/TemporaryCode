package filter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * bit array for lock free bloom filter
 */
public class LockFreeBitArray{
    private static final int LONG_ADDRESSABLE_BITS = 6;     // long value has 64 bits
    final AtomicLongArray bitArray;
    final long bitNum;

    static int bits2words(long numBits) {
        return (int) ((numBits - 1) >> 6) + 1;
    }

    LockFreeBitArray(long bitNum){
        this(new long[bits2words(bitNum)]);
    }

    // Used by serialization
    LockFreeBitArray(long[] bitArray) {
        // a long value has 64 bits
        this.bitNum = (long) bitArray.length << 6;
        //System.out.println("debug..., bitNum: " + bitNum);
        this.bitArray = new AtomicLongArray(bitArray);
    }

    void set(long bitIndex) {
        if(get(bitIndex)) {
            return;
        }

        int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
        long mask = 1L << bitIndex; // only cares about low 6 bits of bitIndex

        long oldValue;
        long newValue;
        do {
            oldValue = bitArray.get(longIndex);
            newValue = oldValue | mask;
            if (oldValue == newValue) {
                return;
            }
        } while (!bitArray.compareAndSet(longIndex, oldValue, newValue));

    }

    boolean get(long bitIndex) {
        return (bitArray.get((int) (bitIndex >>> 6)) & (1L << bitIndex)) != 0;
    }

    long getBitNum(){
        return bitNum;
    }

    ByteBuffer serialize(){
        //long occupy 9 bytes, so len * 8
        int longArrayLen = bitArray.length();
        ByteBuffer buffer = ByteBuffer.allocate(longArrayLen << 3);

        for (int i = 0; i <longArrayLen; ++i) {
            buffer.putLong(bitArray.get(i));
        }
        buffer.flip();
        return buffer;
    }

//    static LockFreeBitArray deserialize(ByteBuffer buffer){
//        int longValueNum = buffer.capacity() >> 3;
//        long[] longArray = new long[longValueNum];
//        for (int i = 0; i < longValueNum; ++i) {
//            longArray[i] = buffer.getLong();
//        }
//        return new LockFreeBitArray(longArray);
//    }
}

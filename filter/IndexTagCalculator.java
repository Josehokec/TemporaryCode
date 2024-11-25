package filter;

/*
   Copyright 2016 Mark Gunlogson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

import hasher.XXHash;
import utils.BasicTypeToByteUtils;


/**
 * Hopefully keeping this class as simple as possible will allow JVM to prevent allocating
 * these entirely. Notice that index should less than Integer.MAX_VALUE to avoid out of memory
 * note that here tag = fingerprint
 */
final class BucketAndTag {
    final int index;
    final int tag;

    BucketAndTag(int bucketIndex, int tag) {
        this.index = bucketIndex;
        this.tag = tag;
    }
}

/**
 * calculates tag and bucket indexes for items.
 */
public class IndexTagCalculator implements java.io.Serializable {
    private static final long serialVersionUID = -2052598678199099089L;
    private final int bucketNum;
    private final int tagBits;

    IndexTagCalculator(int bucketNum, int tagBits){
        if(bucketNum <= 1 || (bucketNum & -bucketNum) != bucketNum){
            throw new RuntimeException("Number of buckets (" + bucketNum + ") must be more than 1 and a power of two");
        }

        // here we use 64 bit length of hash value
        if(tagBits > 32 || tagBits < 4){
            throw new RuntimeException("Number of tag bits (" + tagBits + ") should be in [4,32]");
        }

        this.bucketNum = bucketNum;
        this.tagBits = tagBits;
    }

    long getBucketNum() {
        return bucketNum;
    }

    private static int getTotalBitsNeeded(int bucketNum, int tagBits) {
        return getIndexBitsUsed(bucketNum) + tagBits;
    }

    private static int getIndexBitsUsed(int bucketNum) {
        // how many bits of randomness do we need to create a bucketIndex?
        return 64 - Long.numberOfLeadingZeros(bucketNum);
    }

    /**
     * Determines if the chosen hash function is long enough
     * for the table configuration used.
     * note that we use 64 bits hash value
     */
    private static boolean supportedHashConfig(int bucketNum, int tagBits) {
        return getTotalBitsNeeded(bucketNum, tagBits) <= 64;
    }

    /**
     * Generates the Bucket Index and Tag for a given item.
     */
    BucketAndTag generate(long item){
        int tag;
        int bucketIndex;

        long hashCode = XXHash.hash64(BasicTypeToByteUtils.longToBytes(item), 8, 0);
        bucketIndex = getBucketIndex(hashCode);
        tag = getTagValue(hashCode);

        return new BucketAndTag(bucketIndex, tag);
    }

    int altIndex(int bucketIndex, int nonZeroTag) {
        // 0xc4ceb9fe1a85ec53L hash mixing constant from MurmurHash3 ()
        // Similar value used in: https://github.com/efficient/cuckoofilter/
        // 0x5bd1e995 is the hash constant from MurmurHash2
        long altIndex = bucketIndex ^ (nonZeroTag * 0x5bd1e995);
        return hashIndex(altIndex);
    }

    int getTagValue(long hashCode) {
        int tag = (int) (((1 << tagBits) - 1) & hashCode);
        // we should avoid tag = 0
        tag = (tag == 0) ? 1 : tag;
        return tag;
    }

    int getBucketIndex(long hashCode) {
        // we always need to return a bucket index within table range
        // due to bucketNum is a pow of two
        // then we can return low bit
        return hashIndex(hashCode >>> tagBits);
    }

    int hashIndex(long originIndex) {
        // we always need to return a bucket index within table range
        // we can return low bit because bucketNum is a pow of two
        return (int) (originIndex & (bucketNum - 1));
    }

    int getTagBits(){
        return tagBits;
    }
}

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


import java.util.concurrent.ThreadLocalRandom;

/**
 * cuckoo filter table
 * this table is implemented by bit array
 * thus it has low insertion latency
 */
public class CFTable implements java.io.Serializable{
    private static final long serialVersionUID = 4172048932165857538L;
    private final LongBitSet memBlock;
    private final int bitsPerTag;
    private final int bucketNum;

    private CFTable(LongBitSet memBlock, int bitsPerTag, int bucketNum) {
        this.bitsPerTag = bitsPerTag;
        this.memBlock = memBlock;
        this.bucketNum = bucketNum;
    }

    /**
     * Creates a FilterTable, we have made the constructor become private
     * @param bitsPerTag - number of bits needed for each tag
     * @param bucketNum - number of buckets in filter
     * @return - bit array
     */
    static CFTable create(int bitsPerTag, int bucketNum){
        if(bucketNum <= 1 || (bucketNum & -bucketNum) != bucketNum){
            throw new RuntimeException("Number of buckets (" + bucketNum + ") must be more than 1 and a power of two");
        }

        if(bitsPerTag < 4 || bitsPerTag > 32){
            throw new RuntimeException("tagBits (" + bitsPerTag + ") should be in [4,32]");
        }

        int bitsPerBucket = CuckooFilter.BUCKET_SIZE * bitsPerTag;
        long bitSetSize = bitsPerBucket * (long) bucketNum;
        LongBitSet memBlock = new LongBitSet(bitSetSize);
        return new CFTable(memBlock, bitsPerTag, bucketNum);
    }

    /**
     * inserts a tag into an empty position in the chosen bucket.
     * @param bucketIndex - index
     * @param tag - tag
     * @return - true if insert succeeded
     */
    boolean insertToBucket(int bucketIndex, int tag) {
        for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
            // fingerprint is 0 means this slot is vacant
            if (checkTag(bucketIndex, i, 0)) {
                writeTag(bucketIndex, i, tag);
                return true;
            }
        }
        return false;
    }

    /**
     * @param bucketIndex - bucket num
     * @param posInBucket - position of bucket
     * @param tag - fingerprint (here should > 0)
     * @return - the content of this slot is equal to tag
     */
    boolean checkTag(int bucketIndex, int posInBucket, int tag) {
        long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
        for (int i = 0; i < bitsPerTag; i++) {
            // memBlock.get(i + tagStartIdx) != (tag & (1L << i)) != 0
            // two bits: <1, 1> -> true == false, <0, 0> -> false == true
            // two bits: <1, 0> -> true == true,  <0, 1> -> false == false
            if(memBlock.get(i + tagStartIdx) == ((tag & (1L << i)) == 0)){
                return false;
            }
        }
        return true;
    }

    /**
     *  Finds the bit offset in the bitset for a tag
     * @param bucketIndex - the bucket index
     * @param posInBucket - position in bucket
     * @return - bit position
     */
    private long getTagOffset(int bucketIndex, int posInBucket) {
        long bucketHeaderPos = bucketIndex * CuckooFilter.BUCKET_SIZE * bitsPerTag;
        int offset = posInBucket * bitsPerTag;
        return bucketHeaderPos + offset;
    }

    /**
     * Writes a tag to a bucket position
     */
    void writeTag(int bucketIndex, int posInBucket, int tag) {
        // debug
        // System.out.println("[write tag], tag: " + tag + " position: (" + bucketIndex + "," + posInBucket + ")");
        long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
        for (int i = 0; i < bitsPerTag; i++) {
            if ((tag & (1L << i)) != 0) {
                memBlock.set(tagStartIdx + i);
            }
        }
    }

    /**
     * Finds a tag if present in two buckets.
     * @param i1 - first bucket index
     * @param i2 - second bucket index (alternate)
     * @param tag - tag
     * @return true if tag found in one of the buckets
     */
    boolean findTag(int i1, int i2, int tag) {
        for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
            if (checkTag(i1, i, tag) || checkTag(i2, i, tag))
                return true;
        }
        return false;
    }

    /**
     * Deletes an item from the table if it is found in the bucket
     * @param bucketNum - bucket index
     * @param tag - tag
     * @return - true if item was deleted
     */
    boolean deleteFromBucket(int bucketNum, int tag) {
        for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
            if (checkTag(bucketNum, i, tag)) {
                deleteTag(bucketNum, i);
                return true;
            }
        }
        return false;
    }

    /**
     *  Deletes a tag at a specific bucket index and position
     * @param bucketIndex - bucket index
     * @param posInBucket - position in bucket
     */
    void deleteTag(int bucketIndex, int posInBucket) {
        long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
        memBlock.clear(tagStartIdx, tagStartIdx + bitsPerTag);
    }

    /**
     * Replaces a tag in a random position in the given bucket
     * and returns the tag that was replaced.
     * @param curIndex - bucket index
     * @param tag - fingerprint
     * @return - the replaced tag
     */
    int swapRandomTagInBucket(int curIndex, int tag) {
        int randomBucketPosition = ThreadLocalRandom.current().nextInt(CuckooFilter.BUCKET_SIZE);
        return readTagAndSet(curIndex, randomBucketPosition, tag);
    }

    /**
     * Reads a tag and sets the bits to a new tag
     * at same time for max specification
     */
    int readTagAndSet(int bucketIndex, int posInBucket, int newTag) {
        long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
        int readTag = 0;
        long tagEndIdx = tagStartIdx + bitsPerTag;
        int tagPos = 0;
        for (long i = tagStartIdx; i < tagEndIdx; i++) {
            if ((newTag & (1L << tagPos)) != 0) {
                if (memBlock.getAndSet(i)) {
                    readTag |= (1L << tagPos);
                }
            } else {
                if (memBlock.getAndClear(i)) {
                    readTag |= (1L << tagPos);
                }
            }
            tagPos++;
        }
        // debug
        // System.out.println("[exchange tag], tag: " + newTag + " position: (" + bucketIndex + "," + posInBucket + ")");
        return readTag;
    }

    int getBucketNum(){
        return bucketNum;
    }

    /**
     * @return table total byte size
     */
    long getStorageSize(){
        return memBlock.length();
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }

        if(obj == null || obj.getClass() != getClass()){
            return false;
        }

        CFTable that = (CFTable) obj;
        return this.bitsPerTag == that.bitsPerTag &&
                this.memBlock.equals(that.memBlock) &&
                this.bucketNum == that.bucketNum;
    }

    public CFTable copy() {
        return new CFTable(memBlock.clone(), bitsPerTag, bucketNum);
    }

    public void display(){
        if(bitsPerTag != 16){
            System.out.println("cannot debug");
            return ;
        }
        for(long i = 0; i < bucketNum; ++i){
            System.out.print("content of bucket-" + i + ":");
            for(long j = 0; j < CuckooFilter.BUCKET_SIZE; ++j){
                long offset = i * bitsPerTag * CuckooFilter.BUCKET_SIZE + j * bitsPerTag;
                int tag = 0;
                for(int k = 0; k < 16; ++k){
                    if(memBlock.get(offset + k)){
                        tag += (1 << k);
                    }
                }
                System.out.print(" " + tag);
            }
            System.out.println();
        }
    }
}

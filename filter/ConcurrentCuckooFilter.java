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


import java.util.concurrent.atomic.AtomicLong;

/**
 * 带Victim的并发Cuckoo Filter
 */
public class ConcurrentCuckooFilter implements java.io.Serializable{
    private static final long serialVersionUID = -1337735144654851942L;

    static final int MAX_KICK_OUT = 500;
    static int BUCKET_SIZE = 4;

    private static final double LOAD_FACTOR = 0.955;
    private static final double DEFAULT_FP = 0.01;

    final CFTable table;
    final IndexTagCalculator hasher;
    private final AtomicLong count;
    private transient SegmentedBucketLocker bucketLocks;

    private ConcurrentCuckooFilter(IndexTagCalculator hasher, CFTable table, AtomicLong count) {
        this.hasher = hasher;
        this.table = table;
        this.count = count;
        this.bucketLocks = new SegmentedBucketLocker(table.getBucketNum());
    }

    /***
     * Builds a Cuckoo Filter.
     * To Create a Cuckoo filter, construct this then call {@code #build()}.
     */
    public static class Builder {
        private final long maxKeys;
        // optional arguments
        private double fpr = DEFAULT_FP;

        /**
         * Creates a Builder interface for CuckooFilter with the expected number
         * of insertions using the default false positive rate and concurrency.
         * The default false positive rate is 1%.
         * The default concurrency is 16 expected threads.
         * @param maxKeys - the number of expected insertions to the constructed
         */
        public Builder(long maxKeys) {
            if(maxKeys < 1){
                throw new RuntimeException("maxKeys (" + maxKeys + ") must be > 1, increase maxKeys");
            }
            this.maxKeys = maxKeys;
        }

        /**
         * Sets the false positive rate for the filter (default is 1%).
         * @param fpr - false positive rate from 0-1 exclusive.
         * @return - The builder interface
         */
        public Builder setFalsePositiveRate(double fpr) {
            if(fpr <= 0 || fpr > 0.1){
                throw new RuntimeException("fpp (" + fpr + ") should be in (0, 0.1]");
            }
            this.fpr = fpr;
            return this;
        }

        /**
         * Builds and returns a {@code CuckooFilter}.
         * Invalid configurations will fail on this call.
         * @return - Cuckoo filter
         */
        public ConcurrentCuckooFilter build() {
            int tagBits = CFUtils.getBitsPerItemForFpRate(fpr, LOAD_FACTOR);
            int bucketNum = CFUtils.getBucketsNeeded(maxKeys, LOAD_FACTOR, BUCKET_SIZE);
            IndexTagCalculator hasher = new IndexTagCalculator(bucketNum, tagBits);
            CFTable table = CFTable.create(tagBits, bucketNum);
            return new ConcurrentCuckooFilter(hasher, table, new AtomicLong(0));
        }
    }

    /**
     * Gets the current number of items in the Cuckoo filter
     * @return - number of items in filter
     */
    public long getCount() {
        return count.get();
    }

    /**
     * Gets the current load factor of the Cuckoo filter.
     * @return load fraction of total space used, 0-1 inclusive
     */
    public double getLoadFactor() {
        return count.get() / (table.getBucketNum() * (double) BUCKET_SIZE);
    }


    /**
     * Puts an element into this CuckooFilter
     * Note that:
     * 1. the filter should be considered full after insertion failure.
     * 2. inserting the same item more than 8 times will cause an insertion failure.
     * @param item - item to insert into the filter
     * @return - true if the cuckoo filter inserts this item successfully.
     */
    public boolean put(long item) {
        BucketAndTag bucketAndTag = hasher.generate(item);
        int tag = bucketAndTag.tag;
        int bucketIndex = bucketAndTag.index;
        int altBucketIndex = hasher.altIndex(bucketIndex, tag);
        // before writing, we need to lock
        bucketLocks.lockBucketsWrite(bucketIndex, altBucketIndex);
        try {
            if (table.insertToBucket(bucketIndex, tag) || table.insertToBucket(altBucketIndex, tag)) {
                count.incrementAndGet();
                return true;
            }
        } finally {
            bucketLocks.unlockBucketsWrite(bucketIndex, altBucketIndex);
        }

        CFUtils.Victim victim = new CFUtils.Victim(bucketIndex, altBucketIndex, tag);

        int kickNum = 0;
        while(kickNum < MAX_KICK_OUT){
            // repeat kick
            if (trySwapVictimIntoEmptySpot(victim)){
                break;
            }
            kickNum++;
        }

        // count is incremented here because we should never
        // increase count when not locking buckets or victim.
        count.incrementAndGet();
        // if it does not reach MAX_KICK_OUT, we can clear the victim
        if(kickNum == MAX_KICK_OUT){
            return false;
        }

        return true;
    }

    /**
     * if we kicked a tag we need to move it to alternate position,
     * possibly kicking another tag there, repeating the process
     * until we succeed or run out of chances
     * 1. insert our current tag into a position in an already full bucket,
     * 2. then move the tag that we overwrote to it's alternate index.
     */
    private boolean trySwapVictimIntoEmptySpot(CFUtils.Victim victim) {
        int curIndex = victim.getBucketIndex2();
        // lock bucket. We always use I2 since victim tag is from bucket I1
        bucketLocks.lockSingleBucketWrite(curIndex);
        // random swap
        int curTag = table.swapRandomTagInBucket(curIndex, victim.getTag());
        bucketLocks.unlockSingleBucketWrite(curIndex);

        // new victim's I2 is different as long as tag isn't the same
        int altIndex = hasher.altIndex(curIndex, curTag);
        // try to insert the new victim tag in its alternate bucket
        bucketLocks.lockSingleBucketWrite(altIndex);
        try {
            if(table.insertToBucket(altIndex, curTag)) {
                return true;
            }else{
                // still have a victim, but a different one, update
                // ensure it has the write lock
                victim.setTag(curTag);
                // new victim always shares I1 with previous victims' I2
                victim.setBucketIndex1(curIndex);
                victim.setBucketIndex2(altIndex);
            }
        } finally {
            bucketLocks.unlockSingleBucketWrite(altIndex);
        }
        return false;
    }

    /**
     * @param item - key
     * @return - true if the item might be in the filter
     */
    public boolean contains(long item) {
        BucketAndTag bucketAndTag = hasher.generate(item);
        int i1 = bucketAndTag.index;
        int i2 = hasher.altIndex(bucketAndTag.index, bucketAndTag.tag);
        if (table.findTag(i1, i2, bucketAndTag.tag)) {
            return true;
        }
        return false;
    }

    public int getTagLength(){
        return hasher.getTagBits();
    }

    public void debug(){
        System.out.println("inserted keys: " + count.get());
        table.display();
        System.out.println("number of buckets: " + table.getBucketNum());
        System.out.println("length of fingerprint: " + hasher.getTagBits());
        System.out.println("number of locks: " + bucketLocks.getConcurrentSegments());
        System.out.println("load factor: " + getLoadFactor());

    }

    public void showFalseNegativeInfo(long item){
        BucketAndTag bucketAndTag = hasher.generate(item);
        int tag = bucketAndTag.tag;
        int bucketIndex1 = bucketAndTag.index;
        int bucketIndex2 = hasher.altIndex(bucketIndex1, tag);
        System.out.println("item: " + item + " i1:" + bucketIndex1 + " i2:" + bucketIndex2 + " tag:" + tag);
    }

    public static void main(String[] args) throws Exception{
        //Cuckoo filter testing, number of items: 8000000 threads: 2
        //average insertion latency 356 ns
        //Cuckoo filter testing, number of items: 8000000 threads: 4
        //average insertion latency 186 ns
        //Cuckoo filter testing, number of items: 8000000 threads: 8
        //average insertion latency 133 ns
        //Cuckoo filter testing, number of items: 8000000 threads: 16
        //average insertion latency 117 ns

        class InsertionThread implements Runnable {
            final ConcurrentCuckooFilter ccf;
            final private int[] dataset;

            public InsertionThread(ConcurrentCuckooFilter ccf, int[] dataset) {
                this.ccf = ccf;
                this.dataset = dataset;
            }

            @Override
            public void run() {
                for(int item : dataset){
                    if(!ccf.put(item)){
                        System.out.println("insert item (" + item + ")fails");
                    }
                }
            }
        }

        int threadNum = 8;
        int itemNum = 8_000_000;
        int singleThreadItemNum = itemNum / threadNum;
        System.out.println("Cuckoo filter testing, number of items: " + itemNum + " threads: " + threadNum);

        ConcurrentCuckooFilter ccf = new ConcurrentCuckooFilter.Builder(itemNum).setFalsePositiveRate(0.00004).build();
        int[][] datasets = new int[threadNum][singleThreadItemNum];
        int itemKey = 0;
        for(int i = 0; i < threadNum; ++i){
            for(int j = 0; j < singleThreadItemNum; ++j){
                datasets[i][j] = itemKey++;
            }
        }

        // create multiple threads
        Thread[] threads = new Thread[threadNum];
        for(int i = 0; i < threadNum; ++i){
            threads[i] = new Thread(new InsertionThread(ccf, datasets[i]));
        }

        // start inserting
        long insertionStart = System.nanoTime();
        for(int i = 0; i < threadNum; ++i){
            threads[i].start();
        }
        // wait all threads finish insertion
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long insertionEnd = System.nanoTime();
        System.out.println("average insertion latency " + (insertionEnd - insertionStart)/itemNum + " ns");

        for(int item = 0; item < itemNum; ++item){
            if(!ccf.contains(item)){
                System.out.println("debug-false negative key: " + item);
            }
        }

    }
}

/*
public boolean delete(long item) {
    BucketAndTag bucketAndTag = hasher.generate(item);
    int i1 = bucketAndTag.index;
    int tag = bucketAndTag.tag;
    int i2 = hasher.altIndex(bucketAndTag.index, tag);

    bucketLocker.lockBucketsWrite(i1, i2);
    boolean deleteSuccess = false;
    try {
        if (table.deleteFromBucket(i1, tag) || table.deleteFromBucket(i2, tag))
            deleteSuccess = true;
    } finally {
        bucketLocker.unlockBucketsWrite(i1, i2);
    }
    // try to insert the victim again if we were able to delete an item
    if (deleteSuccess) {
        count.decrementAndGet();
        // might as well try to insert again
        insertCachedVictim();
        return true;
    }
    // if delete failed, but we have a victim, check if the item we're trying
    // to delete IS actually the victim
    long victimLockStamp = writeLockVictimIfSet();
    if (victimLockStamp == 0L)
        return false;
    else {
        try {
            // check victim cache, rewriting
            if (victim.getTag() == tag && victim.getBucketIndex1() == i1 && victim.getBucketIndex2() ==i2) {
                hasVictim = false;
                count.decrementAndGet();
                return true;
            } else
                return false;
        } finally {
            victimLock.unlock(victimLockStamp);
        }
    }
}
*/
/**
 * Attempts to insert the victim item if it exists.
 * inserting from the victim cache to the main table do not affect the count
 * since items in the victim cache are technically still in the table
 */
/*
private void insertCachedVictim() {
    long victimLockStamp = writeLockVictimIfSet();
    if (victimLockStamp == 0L)
        return;
    try {
        int bucketIndex1 = victim.getBucketIndex1();
        int bucketIndex2 = victim.getBucketIndex2();
        int tag = victim.getTag();
        // when we get here we definitely have a victim and a write lock
        bucketLocker.lockBucketsWrite(bucketIndex1, bucketIndex2);
        try {
            if (table.insertToBucket(bucketIndex1, tag) || table.insertToBucket(bucketIndex2, tag)) {
                // set this here because we already have lock
                hasVictim = false;
            }
        } finally {
            bucketLocker.unlockBucketsWrite(bucketIndex1, bucketIndex2);
        }
    } finally {
        victimLock.unlock(victimLockStamp);
    }
}
*/
/***
 * Checks if the victim is set using a read lock and upgrades to a write lock if it is.
 * Will either return a write lock stamp if victim is set, or zero if no victim.
 * @return a write lock stamp for the Victim or 0 if no victim
 */
/*
private long writeLockVictimIfSet() {
    long victimLockstamp = victimLock.readLock();
    if (hasVictim) {
        // try to upgrade our read lock to write exclusive if victim
        long writeLockStamp = victimLock.tryConvertToWriteLock(victimLockstamp);
        // could not get write lock
        if (writeLockStamp == 0L) {
            // so unlock the victim
            victimLock.unlock(victimLockstamp);
            // now just block until we have exclusive lock
            victimLockstamp = victimLock.writeLock();
            // make sure victim is still set with our new write lock
            if (!hasVictim) {
                // victim has been cleared by another thread... so just give
                // up our lock
                victimLock.tryUnlockWrite();
                return 0L;
            } else
                return victimLockstamp;
        } else {
            return writeLockStamp;
        }
    } else {
        victimLock.unlock(victimLockstamp);
        return 0L;
    }
}
*/
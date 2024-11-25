package filter;


import java.util.concurrent.locks.StampedLock;

final class SegmentedBucketLocker {
    private final StampedLock[] lockAry;
    // must be a power of 2 so no modulo bias
    private final int concurrentSegments;

    SegmentedBucketLocker(int bucketNum) {
        concurrentSegments = Math.min(bucketNum, (1 << 16));
        this.lockAry = new StampedLock[concurrentSegments];
        for (int i = 0; i < lockAry.length; i++) {
            lockAry[i] = new StampedLock();
        }
    }

    /**
     * returns the segment that bucket index belongs to
     */
    private int getBucketLock(long bucketIndex) {
        return (int) (bucketIndex % concurrentSegments);
    }

    /**
     * Locks segments corresponding to bucket indexes in specific order to prevent deadlocks
     */
    void lockBucketsWrite(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);
        // always lock segments in same order to avoid deadlocks
        if (bucket1LockIdx < bucket2LockIdx) {
            lockAry[bucket1LockIdx].writeLock();
            lockAry[bucket2LockIdx].writeLock();
        } else if (bucket1LockIdx > bucket2LockIdx) {
            lockAry[bucket2LockIdx].writeLock();
            lockAry[bucket1LockIdx].writeLock();
        }else {
            // if we get here both indexes are on same segment so only lock once!!!
            lockAry[bucket1LockIdx].writeLock();
        }
    }

    /**
     * Locks segments corresponding to bucket indexes in specific order to prevent deadlocks
     */
    void lockBucketsRead(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);
        // always lock segments in same order to avoid deadlocks
        if (bucket1LockIdx < bucket2LockIdx) {
            lockAry[bucket1LockIdx].readLock();
            lockAry[bucket2LockIdx].readLock();
        } else if (bucket1LockIdx > bucket2LockIdx) {
            lockAry[bucket2LockIdx].readLock();
            lockAry[bucket1LockIdx].readLock();
        }else {
            // if we get here both indexes are on same segment so only lock once!!!
            lockAry[bucket1LockIdx].readLock();
        }
    }

    /**
     * Unlocks segments corresponding to bucket indexes in specific order to prevent deadlocks
     */
    void unlockBucketsWrite(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);
        // always unlock segments in same order to avoid deadlocks
        if (bucket1LockIdx == bucket2LockIdx) {
            lockAry[bucket1LockIdx].tryUnlockWrite();
            return;
        }
        lockAry[bucket1LockIdx].tryUnlockWrite();
        lockAry[bucket2LockIdx].tryUnlockWrite();
    }

    void lockSingleBucketWrite(long i1) {
        int bucketLockIdx = getBucketLock(i1);
        lockAry[bucketLockIdx].writeLock();
    }

    void unlockSingleBucketWrite(long i1) {
        int bucketLockIdx = getBucketLock(i1);
        lockAry[bucketLockIdx].tryUnlockWrite();
    }

    void unlockBucketsRead(long i1, long i2) {
        // Unlocks segments corresponding to bucket indexes in specific order to prevent deadlocks
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);
        // always unlock segments in same order to avoid deadlocks
        if (bucket1LockIdx == bucket2LockIdx) {
            lockAry[bucket1LockIdx].tryUnlockRead();
            return;
        }
        lockAry[bucket1LockIdx].tryUnlockRead();
        lockAry[bucket2LockIdx].tryUnlockRead();
    }

    int getConcurrentSegments(){
        return concurrentSegments;
    }
}


/*
void unlockAllBucketsRead() {
    // Unlocks all segments
    for (StampedLock lock : lockAry) {
        lock.tryUnlockRead();
    }
}

void lockAllBucketsRead() {
    // Locks all segments in specific order to prevent deadlocks
    for (StampedLock lock : lockAry) {
        lock.readLock();
    }
}



    void unlockSingleBucketRead(long i1) {
        int bucketLockIdx = getBucketLock(i1);
        lockAry[bucketLockIdx].tryUnlockRead();
    }
void lockSingleBucketRead(long i1) {
        int bucketLockIdx = getBucketLock(i1);
        lockAry[bucketLockIdx].readLock();
    }

 */

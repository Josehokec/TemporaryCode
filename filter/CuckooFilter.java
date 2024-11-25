package filter;

/**
 * cuckoo filter without lock
 * single thread
 */
public class CuckooFilter {

    private static final long serialVersionUID = -1337735144654851942L;
    static final int MAX_KICK_OUT = 500;
    static int BUCKET_SIZE = 4;
    private static final double LOAD_FACTOR = 0.955;
    private static final double DEFAULT_FP = 0.01;

    final CFTable table;
    final IndexTagCalculator hasher;
    private int count;
    private CFUtils.Victim victim;

    public CuckooFilter(CFTable table, IndexTagCalculator hasher) {
        this.table = table;
        this.hasher = hasher;
        this.count = 0;
        this.victim = new CFUtils.Victim(-1, -1, -1);
    }

    /***
     * Builds a Cuckoo Filter.
     * To Create a Cuckoo filter, construct this then call {@code #build()}.
     */
    public static class Builder {
        // required arguments

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
        public CuckooFilter build() {
            int tagBits = CFUtils.getBitsPerItemForFpRate(fpr, LOAD_FACTOR);
            int bucketNum = CFUtils.getBucketsNeeded(maxKeys, LOAD_FACTOR, BUCKET_SIZE);
            IndexTagCalculator hasher = new IndexTagCalculator(bucketNum, tagBits);
            CFTable table = CFTable.create(tagBits, bucketNum);
            return new CuckooFilter(table, hasher);
        }
    }

    /**
     * Gets the current number of items in the Cuckoo filter
     * @return - number of items in filter
     */
    public long getCount() {
        return count;
    }

    /**
     * Gets the current load factor of the Cuckoo filter.
     * @return load fraction of total space used, 0-1 inclusive
     */
    public double getLoadFactor() {
        return count / (table.getBucketNum() * (double) BUCKET_SIZE);
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
        int curTag = bucketAndTag.tag;
        int curIndex = bucketAndTag.index;
        int altIndex = hasher.altIndex(curIndex, curTag);

        // find vacant position, and insert it
        if (table.insertToBucket(curIndex, curTag) || table.insertToBucket(altIndex, curTag)) {
            count++;
            return true;
        }

        if(victim.getTag() != -1){
            System.out.println("Cuckoo filter cannot insert any more because it reaches its capability");
            return false;
        }

        // cache to victim, and swap
        victim.setTag(curTag);
        victim.setBucketIndex1(curIndex);
        victim.setBucketIndex2(altIndex);
        int kickNum = 0;
        while(kickNum < MAX_KICK_OUT){
            // repeat kick
            if (trySwapVictimIntoEmptySpot()){
                break;
            }
            kickNum++;
        }
        count++;
        // if it does not reach MAX_KICK_OUT, we can clear the victim
        if(kickNum != MAX_KICK_OUT){
            victim.setTag(-1);
        }
        return true;
    }

    /**
     * if we kicked a tag we need to move it to alternate position,
     * possibly kicking another tag there, repeating the process
     * until we succeed or run out of chances
     * Step 1. insert our current tag into a position in an already full bucket,
     * Step 2. then move the tag that we overwrote to it's alternate index.
     */
    private boolean trySwapVictimIntoEmptySpot() {
        int curIndex = victim.getBucketIndex2();
        // random swap
        int curTag = table.swapRandomTagInBucket(curIndex, victim.getTag());
        // new victim's I2 is different as long as tag isn't the same
        int altIndex = hasher.altIndex(curIndex, curTag);

        // try to insert the new victim tag in its alternate bucket
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
        // we still need to check victim cache
        return checkVictimCache(bucketAndTag);
    }

    private boolean checkVictimCache(BucketAndTag tagToCheck) {
        int tag = victim.getTag();
        if(tag == -1) {
            return false;
        }
        if (tag == tagToCheck.tag && (tagToCheck.index == victim.getBucketIndex1() || tagToCheck.index == victim.getBucketIndex2())) {
            return true;
        }
        return false;
    }

    public void debug(){
        System.out.println("inserted keys: " + count);
        System.out.println("number of buckets: " + table.getBucketNum());
        System.out.println("length of fingerprint: " + hasher.getTagBits());
    }
}

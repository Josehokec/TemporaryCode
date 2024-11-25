package filter;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * Cuckoo Filter (CF) Utils
 */
public final class CFUtils {

    /**
     * when the filter becomes completely full, the last item that fails to be repositioned
     * will be left without a home. We need to store it to avoid a false negative.
     * Note that we use copy on write here since reads are more common than writes.
     */
    static class Victim implements Serializable {
        private static final long serialVersionUID = -984233593241086192L;

        private int bucketIndex1;
        private int bucketIndex2;
        private int tag;

        // Victim() { tag = -1; }

        Victim(int bucketIndex, int altIndex, int tag) {
            this.bucketIndex1 = bucketIndex;
            this.bucketIndex2 = altIndex;
            this.tag = tag;
        }

        int getBucketIndex1() {
            return bucketIndex1;
        }

        void setBucketIndex1(int bucketIndex1) {
            this.bucketIndex1 = bucketIndex1;
        }

        int getBucketIndex2() {
            return bucketIndex2;
        }

        void setBucketIndex2(int bucketIndex2) {
            this.bucketIndex2 = bucketIndex2;
        }

        int getTag() {
            return tag;
        }

        void setTag(int tag) {
            this.tag = tag;
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketIndex1, bucketIndex2, tag);
        }

        @Override
        public boolean equals(@Nullable Object object) {
            if (object == this) {
                return true;
            }
            if (object instanceof CFUtils.Victim) {
                CFUtils.Victim that = (CFUtils.Victim) object;
                return (this.bucketIndex1 == that.bucketIndex1 || this.bucketIndex1 == that.bucketIndex2) && this.tag == that.tag;
            }
            return false;
        }

        Victim copy() {
            return new Victim(bucketIndex1, bucketIndex2, tag);
        }
    }

    /**
     * Calculates how many bits are needed to reach a given false positive rate.
     * @param fpr - the false positive probability.
     * @return - the length of the tag needed (in bits) to reach the false positive rate.
     */
    static int getBitsPerItemForFpRate(double fpr,double loadFactor) {
        /*
         * equation from Cuckoo Filter: Practically Better Than Bloom Bin Fan,
         * David G. Andersen, Michael Kaminsky , Michael D. Mitzenmacher
         */
        // [\log_2(1/fpr) + 3] / loadFactor
        double logTwoBaseVal = Math.log((1 / fpr) + 3) / Math.log(2);
        return (int) Math.ceil(logTwoBaseVal / loadFactor);
    }

    /**
     * Calculates how many buckets are needed to hold the chosen number of keys,
     * taking the standard load factor into account.
     * @param maxKeys - number of keys the filter is expected to hold before insertion failure.
     * @return - number of buckets needed
     */
    static int getBucketsNeeded(long maxKeys,double loadFactor,int bucketSize) {
        /*
         * force a power-of-two bucket count so hash functions for bucket index
         * can hashBits%numBuckets and get randomly distributed index. See wiki
         * "Modulo Bias". Only time we can get perfectly distributed index is
         * when numBuckets is a power of 2.
         */
        long bucketsNeeded = (int) Math.ceil((1.0 / loadFactor) * maxKeys / bucketSize);
        // get next biggest power of 2
        long curBucketNum = Long.highestOneBit(bucketsNeeded);
        if (bucketsNeeded > curBucketNum)
            curBucketNum = curBucketNum << 1;
        if(curBucketNum > Integer.MAX_VALUE){
            throw new RuntimeException("too large number of buckets, it will incur out of memory exception");
        }
        return (int) curBucketNum;
    }
}

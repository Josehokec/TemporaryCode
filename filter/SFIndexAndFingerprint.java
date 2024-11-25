package filter;


/**
 * this class cannot be extended
 */
final class SFIndexAndFingerprint {
    final int index;
    final int fingerprint;

    SFIndexAndFingerprint(int bucketIndex, int fingerprint) {
        this.index = bucketIndex;
        this.fingerprint = fingerprint;
    }
}

package demo;

public class WindowTag{
    private long windowKey;
    // interval_marker (4 bits) + hit_marker (4 bits)
    private byte marker;

    public WindowTag(long windowKey, byte marker) {
        this.windowKey = windowKey;
        this.marker = marker;
    }

    public long getWindowKey() {
        return windowKey;
    }

    public void setWindowKey(long windowKey) {
        this.windowKey = windowKey;
    }

    public byte getMarker() {
        return marker;
    }

    public void setMarker(byte marker) {
        this.marker = marker;
    }

    /**
     * notice that: startTime + queryWindow <= endTime
     * @param startTime - replay interval's start time
     * @param endTime - replay interval's end time
     * @param queryWindow - query window
     * @return - keys
     */
    public static WindowTag[] getWindowTags(long startTime, long endTime, long queryWindow){
        //assert: endTime - startTime == queryWindow or 2 * queryWindow
        if(endTime - startTime != queryWindow && endTime - startTime != 2 * queryWindow){
            throw new RuntimeException("non standard replay interval: [" + startTime + ", " + endTime + "]");
        }

        // note that we have: startTime + queryWindow <= endTime
        long startKey = startTime / queryWindow;
        long distance1 = startTime - (startKey * queryWindow);
        // notice that: startKey + 1 <= endKey
        long endKey = endTime/ queryWindow;

        WindowTag headTag;
        long quarterWindow = (queryWindow >> 2);
        if(distance1 < quarterWindow){
            // 1111,0000
            headTag = new WindowTag(startKey, (byte) 0xf0);
        }else if(distance1 < (quarterWindow << 1)){
            // 0111,0000
            headTag = new WindowTag(startKey, (byte) 0x70);
        }else if(distance1 < (quarterWindow * 3)){
            // 0011,0000
            headTag = new WindowTag(startKey, (byte) 0x30);
        }else{
            // 0001,0000
            headTag = new WindowTag(startKey, (byte) 0x10);
        }

        WindowTag rearTag;
        long distance2 = endTime - (endKey * queryWindow);
        if(distance2 < quarterWindow){
            // 1000,0000
            rearTag = new WindowTag(endKey, (byte) 0x80);
        }else if(distance2 < (quarterWindow << 1)){
            // 1100,0000
            rearTag = new WindowTag(endKey, (byte) 0xc0);
        }else if(distance1 < (quarterWindow * 3)){
            // 1110,0000
            rearTag = new WindowTag(endKey, (byte) 0xe0);
        }else{
            // 1111,0000
            rearTag = new WindowTag(endKey, (byte) 0xf0);
        }

        WindowTag[] tags;
        // at least 2 keys, at most 3 keys
        if(endKey == startKey + 1){
            tags = new WindowTag[2];
            tags[0] = headTag;
            tags[1] = rearTag;

        }else{
            tags = new WindowTag[3];
            tags[0] = headTag;
            // middle key must be <startKey + 1, 11110000>
            tags[1] = new WindowTag(startKey + 1, (byte) 0xf0);
            tags[2] = rearTag;
        }

        return tags;
    }

    @Override
    public int hashCode(){
        return  (int)(windowKey ^ (windowKey >>> 32));
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }

        if(obj == null || obj.getClass() != getClass()){
            return false;
        }
        WindowTag tag = (WindowTag) obj;
        return windowKey == tag.getWindowKey() && marker == tag.getMarker();
    }
}

//package demo;
//
//
//import event.Event;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//
//public class ReplayIntervals {
//    private boolean outOfOrder;
//    private List<TimeInterval> intervals;
//
//    static class TimeInterval{
//        private long startTime;
//        private long endTime;
//
//        public TimeInterval(long startTime, long endTime) {
//            this.startTime = startTime;
//            this.endTime = endTime;
//        }
//
//        public long getStartTime() {
//            return startTime;
//        }
//
//        public void setStartTime(long startTime) {
//            this.startTime = startTime;
//        }
//
//        public long getEndTime() {
//            return endTime;
//        }
//
//        public void setEndTime(long endTime) {
//            this.endTime = endTime;
//        }
//
//        public boolean include(long timestamp){
//            return timestamp >= startTime && timestamp <= endTime;
//        }
//
//        @Override
//        public boolean equals(Object obj){
//            if(this == obj){
//                return true;
//            }
//
//            if(obj == null || obj.getClass() != getClass()){
//                return false;
//            }
//
//            TimeInterval interval = (TimeInterval) obj;
//            if(interval.getStartTime() == startTime && interval.getEndTime() == endTime){
//                return true;
//            }
//            return false;
//        }
//
//        @Override
//        public String toString(){
//            return "[" + startTime + "," + endTime + "]";
//        }
//    }
//
//    public ReplayIntervals() {
//        outOfOrder = false;
//        intervals = new ArrayList<>(4096);
//    }
//
//    public ReplayIntervals(int size) {
//        outOfOrder = false;
//        intervals = new ArrayList<>(size);
//    }
//
//    /**
//     * note that call this function need to ensure outOfOrder is false
//     * otherwise, you should call sortAndReconstruct function before use this function
//     * @param timestamp - timestamp
//     * @return - this timestamp whether is included by replay interval
//     */
//    public boolean include(long timestamp){
//        int size = intervals.size();
//
//        if(size == 0){
//            return false;
//        }
//
//        if(outOfOrder){
//            sortAndReconstruct();
//            // note that we need to update size and outOfOrder
//            size = intervals.size();
//            outOfOrder = false;
//        }
//        int left = 0;
//        int right = size - 1;
//        int middle = ((left + right) >> 1);
//
//
//        while(left <= right){
//            TimeInterval curInterval = intervals.get(middle);
//            if(curInterval.include(timestamp)){
//                return true;
//            }else{
//                if(timestamp < curInterval.getStartTime()){
//                    right = middle - 1;
//                }else{
//                    left = middle + 1;
//                }
//                middle = ((left + right) >> 1);
//            }
//        }
//        return false;
//    }
//
//    /**
//     * we need to debug
//     * @param events - events
//     * @param eventInOrder - mark the events whether is in-order
//     * @return - filtered events
//     */
//    public List<Event> filtering(List<Event> events, boolean eventInOrder){
//        List<Event> filteredEvents = new ArrayList<>(events.size());
//        if(eventInOrder){
//            // we can use a O(n) complexity of algorithm to filtering
//            int size = intervals.size();
//
//            if(size == 0){
//                return new ArrayList<>();
//            }
//
//            if(outOfOrder){
//                sortAndReconstruct();
//                // note that we need to update size and outOfOrder
//                size = intervals.size();
//                outOfOrder = false;
//            }
//
//            // we use ptr to mark previous position
//            int ptr = 0;
//            for(Event event : events){
//                long ts = event.getTimestamp();
//                // we directly skip
//                while(ptr < size && intervals.get(ptr).getEndTime() < ts){
//                    ptr++;
//                }
//
//                if(ptr >= size){
//                    break;
//                }
//
//                // at this time, intervals.get(ptr).getEndTime() >= ts
//                // then we only need startTime <= ts
//                if(intervals.get(ptr).getStartTime() <= ts){
//                    filteredEvents.add(event);
//                }
//            }
//        }else{
//            // before call include function, we need to ensure replay intervals is in order
//            if(outOfOrder){
//                sortAndReconstruct();
//                // note that we need to update outOfOrder
//                outOfOrder = false;
//            }
//
//            for(Event event : events){
//                if(include(event.getTimestamp())){
//                    filteredEvents.add(event);
//                }
//            }
//        }
//
//        return filteredEvents;
//    }
//
//    public void insert(long startTime, long endTime){
//        int size = intervals.size();
//        if(size == 0 || outOfOrder){
//            intervals.add(new TimeInterval(startTime, endTime));
//        }else{
//            // we need to check whether in-order
//            TimeInterval previousTimeInterval = intervals.get(size - 1);
//            long previousStartTime = previousTimeInterval.getStartTime();
//            long previousEndTime = previousTimeInterval.getEndTime();
//            if(previousStartTime <= startTime){
//                // if in-order, we check whether we need to merge
//                if(previousEndTime >= startTime){
//                    // merge and update
//                    intervals.set(size - 1, new TimeInterval(previousStartTime, Math.max(previousEndTime, endTime)));
//                }else{
//                    // insert
//                    intervals.add(new TimeInterval(startTime, endTime));
//                }
//            }else{
//                outOfOrder = true;
//                intervals.add(new TimeInterval(startTime, endTime));
//            }
//        }
//    }
//
//    /**
//     * when we insert all intervals, we need to call this function to ensure it is sorted
//     */
//    public void sortAndReconstruct(){
//        int size = intervals.size();
//        if(size == 0 || !outOfOrder){
//            return;
//        }
//
//        intervals.sort(Comparator.comparingLong(TimeInterval::getStartTime));
//        List<TimeInterval> updatedIntervals = new ArrayList<>(size);
//
//        TimeInterval firstInterval = intervals.get(0);
//        long previousStartTime = firstInterval.getStartTime();
//        long previousEndTime = firstInterval.getEndTime();
//        for(int i = 1; i < size; ++i){
//            TimeInterval currentInterval = intervals.get(i);
//            long curStartTime = currentInterval.getStartTime();
//            long curEndTime = currentInterval.getEndTime();
//            // if two intervals do not overlap, we store previous interval, and update previous[StartTime/EndTime]
//            if(previousEndTime < curStartTime){
//                updatedIntervals.add(new TimeInterval(previousStartTime, previousEndTime));
//                previousStartTime = curStartTime;
//            }
//            previousEndTime = curEndTime;
//        }
//        updatedIntervals.add(new TimeInterval(previousStartTime, previousEndTime));
//        // update replay intervals
//        intervals = updatedIntervals;
//        outOfOrder = false;
//    }
//
//    public List<TimeInterval> getIntervals(){
//        return intervals;
//    }
//
//    public boolean equals(ReplayIntervals comparedIntervals){
//        List<TimeInterval> compareIntervals = comparedIntervals.getIntervals();
//        int size = intervals.size();
//        if(size != compareIntervals.size()){
//            return false;
//        }
//        for(int i = 0; i < size; ++i){
//            TimeInterval interval = intervals.get(i);
//            if(!interval.equals(compareIntervals.get(i))){
//                return false;
//            }
//        }
//        return true;
//    }
//
//    public void display(){
//        int size = intervals.size();
//        System.out.print("replay intervals:");
//        for(int i = 0; i < size; ++i){
//            System.out.print(" " + intervals.get(i));
//        }
//        System.out.println(", outOfOrder? " + outOfOrder);
//    }
//
//    public byte[] serialize() {
//        // before serializing, we need to ensure intervals are ordered
//        if(outOfOrder){
//            sortAndReconstruct();
//        }
//
//        // format: length (L), startTime_1, endTime_1, ..., startTime_L, ..., endTime_L
//        int size = intervals.size();
//        // byteSize = size * 16
//        int byteSize = 4 + (size << 4 );
//        ByteBuffer buffer = ByteBuffer.allocate(byteSize);
//        buffer.putInt(size);
//        for(int i = 0; i < size; ++i){
//            TimeInterval interval = intervals.get(i);
//            buffer.putLong(interval.getStartTime());
//            buffer.putLong(interval.getEndTime());
//        }
//        buffer.flip();
//        return buffer.array();
//    }
//
//    public static ReplayIntervals deserialize(byte[] content){
//        ByteBuffer buffer = ByteBuffer.wrap(content);
//        int size = buffer.getInt();
//        ReplayIntervals replayIntervals = new ReplayIntervals(size);
//        for(int i = 0; i < size; ++i){
//            replayIntervals.insert(buffer.getLong(), buffer.getLong());
//        }
//        return replayIntervals;
//    }
//}

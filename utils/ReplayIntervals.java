package utils;

import filter.SFTable12_20;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * replay intervals
 */
public class ReplayIntervals {
    private boolean outOfOrder;
    private List<TimeInterval> intervals;

    public static class TimeInterval{
        private long startTime;
        private long endTime;

        public TimeInterval(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public boolean include(long timestamp){
            return timestamp >= startTime && timestamp <= endTime;
        }

        public boolean overlaps(TimeInterval interval){
            return startTime <= interval.getEndTime() && endTime >= interval.getStartTime();
        }

        public boolean overlaps(long anotherStartTime,  long anotherEndTime){
            return startTime <= anotherEndTime && endTime >= anotherStartTime;
        }

        public TimeInterval union(TimeInterval anotherInterval){
            startTime = Math.min(startTime, anotherInterval.getStartTime());
            endTime = Math.max(endTime, anotherInterval.endTime);
            return this;
        }

        public void union(long anotherStartTime, long anotherEndTime){
            startTime = Math.min(startTime, anotherStartTime);
            endTime = Math.max(endTime, anotherEndTime);
        }

        @Override
        public boolean equals(Object obj){
            if(this == obj){
                return true;
            }

            if(obj == null || obj.getClass() != getClass()){
                return false;
            }

            TimeInterval interval = (TimeInterval) obj;
            return interval.getStartTime() == startTime && interval.getEndTime() == endTime;
        }

        @Override
        public String toString(){
            return "[" + startTime + "," + endTime + "]";
        }
    }

    public ReplayIntervals() {
        outOfOrder = false;
        intervals = new ArrayList<>(512);
    }

    public ReplayIntervals(int size) {
        outOfOrder = false;
        intervals = new ArrayList<>(size);
    }

    public boolean checkOverlap(){
        TimeInterval prevTImeInterval = new TimeInterval(-10, -1);
        for(TimeInterval interval: intervals){
            if(prevTImeInterval.getEndTime() < interval.getStartTime()){
                prevTImeInterval = interval;
            }else{
                System.out.println("interval: " + interval);
                return false;
            }
        }
        return true;
    }

    public void insert(long startTime, long endTime){
        int size = intervals.size();
        if(size == 0 || outOfOrder){
            intervals.add(new TimeInterval(startTime, endTime));
        }else{
            // we need to check whether in-order
            TimeInterval previousTimeInterval = intervals.get(size - 1);
            long previousStartTime = previousTimeInterval.getStartTime();
            long previousEndTime = previousTimeInterval.getEndTime();
            if(previousStartTime <= startTime){
                // if in-order, we check whether we need to merge
                if(previousEndTime >= startTime){
                    // merge and update
                    intervals.set(size - 1, new TimeInterval(previousStartTime, Math.max(previousEndTime, endTime)));
                }else{
                    // insert
                    intervals.add(new TimeInterval(startTime, endTime));
                }
            }else{
                outOfOrder = true;
                intervals.add(new TimeInterval(startTime, endTime));
            }
        }
    }

    /**
     * this function aims to insert a time interval and keep order
     * we use binary search
     * @param startTime start time
     * @param endTime   end time
     */
    public void insertInOrder(long startTime, long endTime){
        if(outOfOrder){
            sortAndReconstruct();
        }

        int size = intervals.size();
        if(size == 0){
            intervals.add(new TimeInterval(startTime, endTime));
            return;
        }
        TimeInterval previousTimeInterval = intervals.get(size - 1);
        long previousStartTime = previousTimeInterval.getStartTime();
        long previousEndTime = previousTimeInterval.getEndTime();

        // directly append time interval
        if(previousEndTime < startTime){
            intervals.add(new TimeInterval(startTime, endTime));
            // insert(startTime, endTime);
        }else if(previousStartTime <= startTime){
            if(previousEndTime < endTime){
                intervals.set(size - 1, new TimeInterval(previousStartTime, endTime));
            }
            // else this interval has included in last time interval
            // we do not need to do additional operation
        }else if(intervals.get(0).getStartTime() > endTime){
            intervals.add(0, new TimeInterval(startTime, endTime));
        } else{
            // we need to find position to insert
            int left = 0;
            int right = size - 1;
            boolean finishInsert = false;
            while(left <= right){
                int mid = (left + right) / 2;
                TimeInterval midTimeInterval = intervals.get(mid);
                if(midTimeInterval.overlaps(startTime, endTime)){
                    midTimeInterval.union(startTime, endTime);
                    // Our scenario can only trigger the merging of two previous intervals at most
                    // so here we simplify code
                    //boolean hasMerged = false;
                    if(mid < size - 1 && midTimeInterval.overlaps(intervals.get(mid + 1))){
                        intervals.set(mid, midTimeInterval.union(intervals.get(mid + 1)));
                        intervals.remove(mid + 1);
                        //hasMerged = true;
                    }

                    if(mid > 0 && midTimeInterval.overlaps(intervals.get(mid - 1))){
                        intervals.set(mid - 1, intervals.get(mid - 1).union(midTimeInterval));
                        intervals.remove(mid);
                        //hasMerged = true;
                    }

                    //if(!hasMerged){
                    //    intervals.set(mid, midTimeInterval);
                    //}
                    finishInsert = true;
                    break;
                }else if(midTimeInterval.getEndTime() < startTime &&
                        intervals.get(mid + 1).getStartTime() > endTime){
                    // without merge, directly insert
                    intervals.add(mid + 1, new TimeInterval(startTime, endTime));
                    finishInsert = true;
                    break;
                }else{
                    if(midTimeInterval.getEndTime() < startTime){
                        left = mid + 1;
                    }else{
                        right = mid - 1;
                    }
                }
            }
            if(!finishInsert){
                System.out.println("has bug...");
            }
        }
    }

    // before calling this function, you should call sortAndReconstruct function
    // when intervals are sorted, then cost is log(N)
    public boolean contains(long ts){
        // binary search (very fast)
        int left = 0;
        int right = intervals.size() - 1;
        while(left <= right){
            int mid = (left + right) / 2;
            TimeInterval interval = intervals.get(mid);
            if(interval.include(ts)){
                return true;
            }
            if(ts < interval.getStartTime()){
                right = mid - 1;
            }else{
                left = mid + 1;
            }
        }
        return false;
    }

    /**
     * when we insert all intervals, we need to call this function to ensure it is sorted
     */
    public void sortAndReconstruct(){
        int size = intervals.size();
        if(size == 0 || !outOfOrder){
            return;
        }

        intervals.sort(Comparator.comparingLong(TimeInterval::getStartTime));
        List<TimeInterval> updatedIntervals = new ArrayList<>(size);

        TimeInterval firstInterval = intervals.get(0);
        long previousStartTime = firstInterval.getStartTime();
        long previousEndTime = firstInterval.getEndTime();
        for(int i = 1; i < size; ++i){
            TimeInterval currentInterval = intervals.get(i);
            long curStartTime = currentInterval.getStartTime();
            long curEndTime = currentInterval.getEndTime();
            // if two intervals do not overlap, we store previous interval, and update previous[StartTime/EndTime]
            if(previousEndTime < curStartTime){
                updatedIntervals.add(new TimeInterval(previousStartTime, previousEndTime));
                previousStartTime = curStartTime;
            }
            previousEndTime = curEndTime;
        }
        updatedIntervals.add(new TimeInterval(previousStartTime, previousEndTime));
        // update replay intervals
        intervals = updatedIntervals;
        outOfOrder = false;
    }

    public List<TimeInterval> getIntervals(){
        return intervals;
    }

    public boolean equals(ReplayIntervals comparedIntervals){
        List<TimeInterval> compareIntervals = comparedIntervals.getIntervals();
        int size = intervals.size();
        if(size != compareIntervals.size()){
            return false;
        }
        for(int i = 0; i < size; ++i){
            TimeInterval interval = intervals.get(i);
            if(!interval.equals(compareIntervals.get(i))){
                return false;
            }
        }
        return true;
    }

    public void intersect(ReplayIntervals ri){
        // assert this.outOfOrder = false
        ri.sortAndReconstruct();
        List<TimeInterval> anotherInterval = ri.getIntervals();

        int idx1 = 0;
        int idx2 = 0;
        int size1 = intervals.size();
        int size2 = anotherInterval.size();
        List<TimeInterval> intersectedIntervals = new ArrayList<>(size1);

        while(idx1 < size1 && idx2 < size2){
            TimeInterval interval1 = intervals.get(idx1);
            TimeInterval interval2 = anotherInterval.get(idx2);

            if(interval1.getEndTime() < interval2.getStartTime()){
                idx1++;
            }else if(interval1.getStartTime() > interval2.getEndTime()){
                idx2++;
            }else{
                // intersect
                long maxStartTime = Math.max(interval1.getStartTime(), interval2.getStartTime());
                if(interval1.getEndTime() > interval2.getEndTime()){
                    idx2++;
                    TimeInterval ti = new TimeInterval(maxStartTime, interval2.getEndTime());
                    intersectedIntervals.add(ti);
                }else{
                    idx1++;
                    TimeInterval ti = new TimeInterval(maxStartTime, interval1.getEndTime());
                    intersectedIntervals.add(ti);
                }
            }
        }
        // without while(idx1 < size1) ... while(idx2 < size2)

        // update
        intervals = intersectedIntervals;
        outOfOrder = false;
        //return this;
    }

    public void union(ReplayIntervals ri){
        // assert this.outOfOrder = false
        ri.sortAndReconstruct();

        List<TimeInterval> anotherInterval = ri.getIntervals();

        int idx1 = 0;
        int idx2 = 0;
        int size1 = intervals.size();
        int size2 = anotherInterval.size();
        ReplayIntervals unionIntervals = new ReplayIntervals(size1 + size2);

        while(idx1 < size1 && idx2 < size2) {
            TimeInterval interval1 = intervals.get(idx1);
            TimeInterval interval2 = anotherInterval.get(idx2);

            if (interval1.getStartTime() < interval2.getStartTime()) {
                unionIntervals.insert(interval1.getStartTime(), interval1.getEndTime());
                idx1++;
            }else{
                unionIntervals.insert(interval2.getStartTime(), interval2.getEndTime());
                idx2++;
            }
        }

        while(idx1 < size1){
            TimeInterval interval1 = intervals.get(idx1);
            unionIntervals.insert(interval1.getStartTime(), interval1.getEndTime());
            idx1++;
        }

        while(idx2 < size2){
            TimeInterval interval2 = anotherInterval.get(idx2);
            unionIntervals.insert(interval2.getStartTime(), interval2.getEndTime());
            idx2++;
        }
        intervals = unionIntervals.getIntervals();
        outOfOrder = false;
        //return this;
    }

    /**
     * this function aims to estimate how many keys will insert into shrink filter
     * due to these intervals are sorted, then we only need O(1) space
     */
    public int getKeyNumber(long window){
        if(outOfOrder){
            sortAndReconstruct();
        }

        long previousKey = -1;
        long keyCnt = 0;
        for (TimeInterval interval : intervals) {
            long startWindowId = interval.startTime / window;
            long endWindowId = interval.endTime / window;

            if (startWindowId == previousKey) {
                keyCnt += (endWindowId - previousKey);
            } else {
                keyCnt += (endWindowId - startWindowId + 1);
            }
            previousKey = endWindowId;
        }

        return (int) keyCnt;
    }

    public void display(){
        int size = intervals.size();
        System.out.print("replay intervals:");
        for(int i = 0; i < size; ++i){
            System.out.print(" " + intervals.get(i));
        }
        System.out.println(", outOfOrder? " + outOfOrder);
    }

    public ByteBuffer serialize() {
        // before serializing, we need to ensure intervals are ordered
        if(outOfOrder){
            sortAndReconstruct();
        }

        // format: length (L), startTime_1, endTime_1, ..., startTime_L, ..., endTime_L
        int size = intervals.size();
        // byteSize = size * 16
        int byteSize = 4 + (size << 4 );
        ByteBuffer buffer = ByteBuffer.allocate(byteSize);
        buffer.putInt(size);
        for(int i = 0; i < size; ++i){
            TimeInterval interval = intervals.get(i);
            buffer.putLong(interval.getStartTime());
            buffer.putLong(interval.getEndTime());
        }
        buffer.flip();
        return buffer;
    }

    public static ReplayIntervals deserialize(ByteBuffer buffer){
        int size = buffer.getInt();
        ReplayIntervals replayIntervals = new ReplayIntervals(size);
        for(int i = 0; i < size; ++i){
            replayIntervals.insert(buffer.getLong(), buffer.getLong());
        }
        return replayIntervals;
    }
}



/*

// when events are ordered, we can use this variable to quick return results
private int readPos;
// before calling this function, you should call sortAndReconstruct function
public boolean prefetchContains(int ts){
    TimeInterval interval = intervals.get(readPos);
    if(interval.include(ts)){
        return true;
    }

    while(interval.getStartTime() > ts && readPos > 0){
        readPos--;
        interval = intervals.get(readPos);
    }

    while(interval.getEndTime() < ts && readPos < intervals.size() - 1){
        readPos++;
        interval = intervals.get(readPos);
    }

    return interval.include(ts);
}

 */
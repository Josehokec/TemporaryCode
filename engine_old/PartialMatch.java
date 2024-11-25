package engine;

import java.util.List;

// notice we do not support Negation, Kleene operators
// in the future, through maintaining a Map<String (VariableName), List<Integer>>,
// we maybe could extend NFA to support Negation, Kleene operators
public class PartialMatch {
    private long startTime;
    private long endTime;
    // List<Event> events;
    private List<Integer> eventPointers;

    PartialMatch(long startTime, long endTime, List<Integer> eventPointers){
        this.startTime = startTime;
        this.endTime = endTime;
        this.eventPointers = eventPointers;
    }

    public long getStartTime(){
        return startTime;
    }

    public long getEndTime(){
        return endTime;
    }

    public void setEndTime(long endTime){
        this.endTime = endTime;
    }

    public List<Integer> getEventPointers(){
        return eventPointers;
    }

    public int getPointer(int index){
        return eventPointers.get(index);
    }

    public void print(EventBuffer eventBuffer){
        String result  = "|";
        for(int i = 0; i < eventPointers.size(); ++i){
            result += eventBuffer.getEvent(eventPointers.get(i));
            result += "|";
        }
        System.out.println(result);
    }

    public String getSingleMatchedResult(EventBuffer eventBuffer){
        String result  = "|";
        for(int i = 0; i < eventPointers.size(); ++i){
            result += eventBuffer.getEvent(eventPointers.get(i));
            result += "|";
        }
        return result;
    }
}

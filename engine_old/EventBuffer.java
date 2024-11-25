package engine;

import event.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * simple version
 * in fact, we should use reference-counter-technique to implement
 * event buffer, then, event buffer will have a small space
 */
public class EventBuffer {
    private List<Event> events;
    private int count;

    EventBuffer(){
        events = new ArrayList<>(1024);
        count = 0;
    }

    public final int insertEvent(Event event){
        events.add(event);
        return count++;
    }

    public final Event getEvent(int index){
        return events.get(index);
    }
}

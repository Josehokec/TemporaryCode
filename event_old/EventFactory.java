package event;

public class EventFactory {
    public Event getEvent(String schemaName){
        switch (schemaName) {
            case "CRIMES":
                return new CrimesEvent();
            case "X":
                return new XEvent();
            default:
                return null;
        }
    }
}

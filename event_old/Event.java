package event;

public abstract class Event {
    protected String eventType;
    protected long timestamp;

    // maybe add time granularity, like HOUR, MINUTER, SECOND,...
    public abstract Event parseString(String line);

    // according to column name to obtain data type
    public abstract DataType getDataType(String columnName);

    public abstract Object getAttributeValue(String columnName);

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

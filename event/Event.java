package event;

import store.DataType;

public abstract class Event {
    // maybe add time granularity, like HOUR, MINUTER, SECOND,...


    // according to column name to obtain data type
    public abstract DataType getDataType(String columnName);
    public abstract Object getAttributeValue(String columnName);
    public abstract String getEventType();
    public abstract long getTimestamp();

}

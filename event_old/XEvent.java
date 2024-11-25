package event;

public class XEvent extends Event{
    // Schema: Type, Attribute1, Attribute2, Timestamp
    private int attribute1;
    private String attribute2;

    // this construct method only be used for parse String
    public XEvent(){}

    @SuppressWarnings("unused")
    public XEvent(String eventType, int attribute1, String attribute2, long timestamp) {
        this.eventType = eventType;
        this.attribute1 = attribute1;
        this.attribute2 = attribute2;
        this.timestamp = timestamp;
    }

    public XEvent(String[] valueList){
        eventType = valueList[0];
        attribute1 = Integer.parseInt(valueList[1]);
        attribute2 = valueList[2];
        timestamp = Long.parseLong(valueList[3]);
    }

    @Override
    public Event parseString(String line) {
        // Type, Attribute1, Attribute2, Timestamp
        String[] valueList = line.split(",");
        int count = valueList.length;
        if(count != 4){
            throw new RuntimeException("this line only contains " + count + " columns (Crimes event has 7 columns), mismatching.");
        }
        return new XEvent(valueList);
    }

    @Override
    public DataType getDataType(String columnName) {
        if(columnName.equalsIgnoreCase("attribute1")){
            return DataType.INT;
        }else if(columnName.equalsIgnoreCase("attribute2")){
            return DataType.STRING;
        }else if(columnName.equalsIgnoreCase("type")){
            return DataType.STRING;
        }else if(columnName.equalsIgnoreCase("timestamp")){
            return DataType.LONG;
        }else{
            throw new RuntimeException("Column name: '" + columnName + "' does not exits");
        }
    }

    @Override
    public Object getAttributeValue(String columnName) {
        if(columnName.equalsIgnoreCase("attribute1")){
            return attribute1;
        }else if(columnName.equalsIgnoreCase("attribute2")){
            return attribute2;
        }else if(columnName.equalsIgnoreCase("type")){
            return eventType;
        }else if(columnName.equalsIgnoreCase("timestamp")){
            return timestamp;
        }else{
            throw new RuntimeException("Column name: '" + columnName + "' does not exits");
        }
    }

    @Override
    public String toString(){
        return eventType + "," + attribute1 + "," + attribute2 + "," + timestamp;
    }
}

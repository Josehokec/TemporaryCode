package event;

/**
 * Crimes原始数据集列数很多，这里只选取了部分列做实验
 */
public class CrimesEvent extends Event{
    public int id;
    public int beat;
    public int district;
    public float latitude;
    public float longitude;

    public CrimesEvent(){}

    @SuppressWarnings("unused")
    public CrimesEvent(String eventType, int id, int beat, int district, float latitude, float longitude, long timestamp){
        this.eventType = eventType;
        this.id = id;
        this.beat = beat;
        this.district = district;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
    }

    public CrimesEvent(String[] valueList){
        // type,id,beat,district,latitude,longitude,timestamp
        this.eventType = valueList[0];
        this.id = Integer.parseInt(valueList[1]);
        this.beat = Integer.parseInt(valueList[2]);
        this.district = Integer.parseInt(valueList[3]);
        this.latitude = Float.parseFloat(valueList[4]);
        this.longitude = Float.parseFloat(valueList[5]);
        this.timestamp = Long.parseLong(valueList[6]);
    }

    public DataType getDataType(String columnName){
        if(columnName.equalsIgnoreCase("id")){
            return DataType.INT;
        }else if(columnName.equalsIgnoreCase("beat")){
            return DataType.INT;
        }else if(columnName.equalsIgnoreCase("district")){
            return DataType.INT;
        }else if(columnName.equalsIgnoreCase("latitude")){
            return DataType.FLOAT;
        }else if(columnName.equalsIgnoreCase("longitude")){
            return DataType.FLOAT;
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
        if(columnName.equalsIgnoreCase("id")){
            return id;
        }else if(columnName.equalsIgnoreCase("beat")){
            return beat;
        }else if(columnName.equalsIgnoreCase("district")){
            return district;
        }else if(columnName.equalsIgnoreCase("latitude")){
            return latitude;
        }else if(columnName.equalsIgnoreCase("longitude")){
            return longitude;
        }else if(columnName.equalsIgnoreCase("type")){
            return eventType;
        }else if(columnName.equalsIgnoreCase("timestamp")){
            return timestamp;
        }else{
            throw new RuntimeException("Column name: '" + columnName + "' does not exits");
        }
    }

    /**
     * read csv file and generate related object
     * @param line      file line
     * @return          crimes event
     */
    public Event parseString(String line){
        // type,id,beat,district,latitude,longitude,timestamp
        String[] valueList = line.split(",");
        int count = valueList.length;
        if(count != 7){
            throw new RuntimeException("this line only contains " + count + " columns (Crimes event has 7 columns), mismatching.");
        }
        return new CrimesEvent(valueList);
    }

    @Override
    public String toString(){
        return eventType + "," + id + "," + beat + "," +
                district + "," + latitude + "," +
                longitude + "," + timestamp;
    }
}

package event;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


/*
Given the following sql
String createSQL = "create table CRIMES(" +
                    "       primary_type VARCHAR(32)," +
                    "       id int," +
                    "       beat int," +
                    "       district int," +
                    "       latitude double," +
                    "       longitude double," +
                    "       timestamp long);";
we store records in fixed length mode
 */
public class CrimesEvent implements Comparable<CrimesEvent>{
    //public static TimeGranularity granularity = TimeGranularity.SECOND;

    // please caution: these variable name should align with event schema
    public String type;
    public int id;
    public int beat;
    public int district;
    public double latitude;
    public double longitude;
    // flink cep need to define local data time rather long number
    public LocalDateTime eventTime;

    // this function is vital for flink cep
    public CrimesEvent(){}

    public CrimesEvent(String type, int id, int beat, int district, double latitude, double longitude, LocalDateTime eventTime) {
        this.type = type;
        this.id = id;
        this.beat = beat;
        this.district = district;
        this.latitude = latitude;
        this.longitude = longitude;
        this.eventTime = eventTime;
    }

    // CrimesEvent
    public static CrimesEvent valueOf(byte[] byteRecord){
        int len = 0;
        for(int i = 0; i < 32; i++){
            if (byteRecord[i] == 0) {
                break;
            }
            len++;
        }
        String eventType = new String(byteRecord, 0, len);
        ByteBuffer buffer = ByteBuffer.wrap(byteRecord);
        // 32,4,4,4,8,8,8
        int id = buffer.getInt(32);
        int beat = buffer.getInt(36);
        int district = buffer.getInt(40);
        double latitude = buffer.getDouble(44);
        double longitude = buffer.getDouble(52);

        Instant instant = Instant.ofEpochMilli(buffer.getLong(60) * 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime eventTime = LocalDateTime.ofInstant(instant, zoneId);
        //type,id,beat,district,latitude,longitude,eventTime
        //ROBBERY,1311667,1924,19,41.9400267,-87.653500908,978323920

        return new CrimesEvent(eventType, id, beat, district, latitude, longitude, eventTime);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getBeat() {
        return beat;
    }

    public void setBeat(int beat) {
        this.beat = beat;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public LocalDateTime getTimestamp() {
        return eventTime;
    }

    public void setTimestamp(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString(){
        return type + "," + id + "," + beat + "," +
                district + "," + latitude + "," +
                longitude + "," + eventTime;
    }

    @Override
    public int compareTo(CrimesEvent o) {
        if (this.eventTime.isEqual(o.getTimestamp())) {
            return Integer.compare(this.id, o.getId());
        }else{
            return this.eventTime.isBefore(o.getTimestamp()) ? -1 : 1;
        }
    }
}

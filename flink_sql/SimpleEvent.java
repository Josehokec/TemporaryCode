package flink_sql;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class SimpleEvent{
    public String type;
    public int id;
    public float value1;
    public long value2;
    public String info;
    public LocalDateTime eventTime;

    @SuppressWarnings("unused")
    public SimpleEvent(){}

    public SimpleEvent(String type, int id, float value1, long value2, String info, long eventTime){
        this.type = type;
        this.id = id;
        this.value1 = value1;
        this.value2 = value2;
        this.info = info;
        Instant instant = Instant.ofEpochMilli(eventTime * 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        this.eventTime = LocalDateTime.ofInstant(instant, zoneId);

    }
    public SimpleEvent(String strLine){
        String[] fields = strLine.split(",");
        this.type = fields[0];
        this.id = Integer.parseInt(fields[1]);
        this.value1 = Float.parseFloat(fields[2]);
        this.value2 = Long.parseLong(fields[3]);
        this.info = fields[4];

        Instant instant = Instant.ofEpochMilli(Long.parseLong(fields[5]) * 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        this.eventTime = LocalDateTime.ofInstant(instant, zoneId);
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

    public float getValue1() {
        return value1;
    }

    public void setValue1(float value1) {
        this.value1 = value1;
    }

    public long getValue2() {
        return value2;
    }

    public void setValue2(long value2) {
        this.value2 = value2;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString(){
        return type + "," + id + "," + value1 + "," + value2 + "," + info + "," + eventTime;
    }
}




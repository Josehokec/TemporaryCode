package flink_sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java. StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 不测不知道 一测吓一跳 FlinkSQL处理这种查询开销很大！！！
 */
public class QueryTest{

    // 这里测试一下查询
    public static TableResult processQuery(DataStream<SimpleEvent> dataStream, Schema schema, String sql, StreamTableEnvironment tEnv){
        try {
            //DataStream<SimpleEvent> dataStream = env.fromElements(events.get(0));
            Table table = tEnv.fromDataStream(dataStream, schema);
            tEnv.createTemporaryView("simple_events", table);
        }catch (Exception e){
            e.printStackTrace();
        }
        TableResult res = tEnv.executeSql(sql);
        tEnv.dropTemporaryView("simple_events");
        return res;
    }

    public static List<SimpleEvent> loadEvents(String filePath){
        List<SimpleEvent> events = new ArrayList<>();
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            while ((line = b.readLine()) != null) {
                events.add(new SimpleEvent(line));
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        return events;
    }

    public static void main(String[] args){
        //long maxHeapSize = Runtime.getRuntime().maxMemory();
        //System.out.println("最大堆内存: " + maxHeapSize / (1024 * 1024) + " MB");

        String dir = System.getProperty("user.dir");
        String sep = File.separator;
        // 10M爆内存了 然后最大堆内存: 7282 MB可以运行

        // event_10000, event_100
        // 运行之前请先生成数据集
        String filePath = dir + sep + "src" + sep + "main" + sep + "dataset" + sep + "event_XXX.csv";

        List<SimpleEvent> events = loadEvents(filePath);
        System.out.println("number of events: " + events.size());

        //System.out.println(events.get(0).toString());
        long start = System.currentTimeMillis();
        Schema schema = Schema.newBuilder()
                .column("type", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("value1", DataTypes.FLOAT())
                .column("value2", DataTypes.BIGINT())
                .column("info", DataTypes.STRING())
                .column("eventTime", DataTypes.TIMESTAMP(3))
                .watermark("eventTime", "eventTime - INTERVAL '0' SECOND")      // 0表示不允许乱序，1表示允许乱序1秒钟
                .build();
        long end = System.currentTimeMillis();
        System.out.println("create schema time: " + (end - start) + "ms");
        String query =
                "SELECT * FROM simple_events MATCH_RECOGNIZE(\n" +
                        "    ORDER BY eventTime\n" +
                        "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                        "    ONE ROW PER MATCH\n" +
                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
                        "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '1' MINUTE\n" +
                        "    DEFINE \n" +
                        "        A AS A.type = 'A' AND A.value1 > 50, \n" +
                        "        B AS B.type = 'B' AND B.value1 > 50, \n" +
                        "        C AS C.type = 'C' AND C.value2 > 50 \n" +
                        ") MR;";

        EnvironmentSettings settings;
        StreamTableEnvironment tEnv = null;
        DataStream<SimpleEvent> dataStream = null;
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
            dataStream = env.fromCollection(events);
        }catch (Exception e){
            e.printStackTrace();
        }

        for(int i =0; i < 10; ++i){
            long startTime = System.currentTimeMillis();
            TableResult res = processQuery(dataStream, schema, query, tEnv);
            long endTime = System.currentTimeMillis();
            //res.print();
            //3s
            System.out.println("start cost : " + (endTime - startTime) + "ms");
        }

        try{
            Thread.sleep(5_000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

/*
创建schema的时候大概10-12ms，这一步省掉

100个事件
start cost : 3007ms
start cost : 811ms
start cost : 744ms
start cost : 713ms
start cost : 713ms
start cost : 704ms
start cost : 757ms
start cost : 679ms
start cost : 678ms
start cost : 666ms

10K事件
start cost : 3161ms
start cost : 959ms
start cost : 800ms
start cost : 748ms
start cost : 741ms
start cost : 711ms
start cost : 775ms
start cost : 696ms
start cost : 695ms
start cost : 685ms

100K事件
start cost : 3330ms
start cost : 1101ms
start cost : 888ms
start cost : 835ms
start cost : 809ms
start cost : 810ms
start cost : 866ms
start cost : 759ms
start cost : 795ms
start cost : 750ms

1M事件
start cost : 4943ms
start cost : 1733ms
start cost : 1321ms
start cost : 1857ms
start cost : 1610ms
start cost : 2227ms
start cost : 1890ms
start cost : 2145ms
start cost : 3107ms
start cost : 2582ms

10M -> 1分钟
85736 rows in set
35840
 */
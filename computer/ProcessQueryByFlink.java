package computer;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.List;

public class ProcessQueryByFlink {
    public static int parallelism = 1;

    public static <T> void processQuery(List<T> events, Schema schema, String sql, String tableName){
        long startTime = System.currentTimeMillis();
        EnvironmentSettings settings;
        StreamTableEnvironment tEnv = null;
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
            DataStream<T> dataStream = env.fromCollection(events);
            Table table = tEnv.fromDataStream(dataStream, schema);
            tEnv.createTemporaryView(tableName, table);
        }catch (Exception e){
            e.printStackTrace();
        }
        TableResult res = tEnv.executeSql(sql);
        try (CloseableIterator<Row> iterator = res.collect()) {
            int rowCount = 0;
            while (iterator.hasNext()) {
                //Row row = iterator.next(); ....
                iterator.next();
                rowCount++;
            }
            System.out.println("sum row number: " + rowCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        tEnv.dropTemporaryView(tableName);
        long endTime = System.currentTimeMillis();
        System.out.println("========>match time: " + (endTime - startTime) + "ms");
    }
}

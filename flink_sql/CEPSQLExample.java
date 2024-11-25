package flink_sql;

// blog URL: https://blog.51cto.com/u_14465598/5625930
// https://mvnrepository.com/artifact/org.apache.flink/flink-table-api-java-bridge
// https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/match_recognize/

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java. StreamTableEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CEPSQLExample {

    public static class Ticker {
        public long id;
        public String symbol;
        public long price;
        public long tax;
        public LocalDateTime rowtime;

        // if we do not have this line, we have Exception as follows:
        // org.apache.flink.table.api.ValidationException: Unable to find a field named 'id' in the physical data type derived from the given type information for schema declaration.
        public Ticker(){}

        public Ticker(long id, String symbol, long price, long tax, LocalDateTime rowtime) {
            this.id = id;
            this.symbol = symbol;
            this.price = price;
            this.tax = tax;
            this.rowtime = rowtime;
            //System.out.println("rowtime: " + rowtime);
        }
    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = null;
        StreamTableEnvironment tEnv = null;
        try {
            //System.out.println("hello world 1");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // env.setParallelism(2);
            settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            // EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);


            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            System.out.println("hello world 2");
            // 这里还是要把它转换成object的
            DataStream<Ticker> dataStream =
                    env.fromElements(
                            new Ticker(1, "Apple", 11, 2, LocalDateTime.parse("2021-12-10 10:00:01", dateTimeFormatter)),
                            new Ticker(2, "Apple", 16, 2, LocalDateTime.parse("2021-12-10 10:00:02", dateTimeFormatter)),
                            new Ticker(3, "Apple", 13, 2, LocalDateTime.parse("2021-12-10 10:00:03", dateTimeFormatter)),
                            new Ticker(4, "Apple", 15, 2, LocalDateTime.parse("2021-12-10 10:00:04", dateTimeFormatter)),
                            new Ticker(5, "Apple", 14, 1, LocalDateTime.parse("2021-12-10 10:00:05", dateTimeFormatter)),
                            new Ticker(6, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:06", dateTimeFormatter)),
                            new Ticker(7, "Apple", 23, 2, LocalDateTime.parse("2021-12-10 10:00:07", dateTimeFormatter)),
                            new Ticker(8, "Apple", 22, 2, LocalDateTime.parse("2021-12-10 10:00:08", dateTimeFormatter)),
                            new Ticker(9, "Apple", 25, 2, LocalDateTime.parse("2021-12-10 10:00:09", dateTimeFormatter)),
                            new Ticker(10, "Apple", 11, 1, LocalDateTime.parse("2021-12-10 10:00:10", dateTimeFormatter)),
                            new Ticker(11, "Apple", 15, 1, LocalDateTime.parse("2021-12-10 10:00:11", dateTimeFormatter)),
                            new Ticker(12, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:12", dateTimeFormatter)),
                            new Ticker(13, "Apple", 25, 1, LocalDateTime.parse("2021-12-10 10:00:13", dateTimeFormatter)),
                            new Ticker(14, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:14", dateTimeFormatter)),
                            new Ticker(15, "Apple", 15, 1, LocalDateTime.parse("2021-12-10 10:00:15", dateTimeFormatter)),
                            new Ticker(16, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:16", dateTimeFormatter)),
                            new Ticker(17, "Apple", 15, 1, LocalDateTime.parse("2021-12-10 10:00:17", dateTimeFormatter))
                    );

            Table table = tEnv.fromDataStream(dataStream, Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("symbol", DataTypes.STRING())
                    .column("price", DataTypes.BIGINT())
                    .column("tax", DataTypes.BIGINT())
                    .column("rowtime", DataTypes.TIMESTAMP(3))
                    .watermark("rowtime", "rowtime - INTERVAL '0' SECOND")
                    .build());
            tEnv.createTemporaryView("Ticker", table);
            String sql =
                    "SELECT * FROM Ticker MATCH_RECOGNIZE (" +
                            " ORDER BY rowtime " +
                            " MEASURES A.id as AID, B.id as BID" +
                            " ONE ROW PER MATCH" +
                            " AFTER MATCH SKIP TO NEXT ROW" +
                            " PATTERN (A N*? B) WITHIN INTERVAL '3' SECOND" +
                            " DEFINE A AS A.price < 19, B AS B.price >= 19" +
                            " ) MR";
            // " PATTERN (A N*? B) WITHIN INTERVAL '3' SECOND"
            //System.out.println("hello world 3");
            TableResult res = tEnv.executeSql(sql);
            System.out.println("hello world 4");
            res.print();
            tEnv.dropTemporaryView("Ticker");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

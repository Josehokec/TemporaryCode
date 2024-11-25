


variableName AS variableName.columnName

### 时间属性
时间属性可以是每个表模式的一部分。
它们是在从CREATE TABLE DDL或DataStream创建表时定义的。
一旦定义了时间属性，它就可以作为字段引用并用于基于时间的操作。
只要时间属性不被修改，并且只是从查询的一部分转发到另一部分，它就仍然是有效的时间属性。
时间属性的行为类似于常规时间戳，并且可以用于计算。当用于计算时，时间属性被具体化并充当标准时间戳。
但是，普通时间戳不能用于代替时间属性或转换为时间属性。

## 事件时间
事件时间允许表程序根据每条记录中的时间戳生成结果，即使发生乱序或延迟事件，也允许结果一致。
它还确保从持久存储中读取记录时表程序结果的可重放性。

此外，事件时间允许批处理和流环境中的表程序使用统一的语法。
流环境中的时间属性可以是批处理环境中行的常规列。


为了处理乱序事件并区分流中的准时事件和延迟事件，
Flink需要知道每一行的时间戳，它还需要定期指示到目前为止处理进展了多长时间（通过所谓的水印）。

### DDL定义

事件时间属性是使用CREATE表DDL中的WATERMARK语句定义的。
水印语句在现有事件时间字段上定义水印生成表达式，该表达式将事件时间字段标记为事件时间属性。


Flink支持在TIMESTAMP列和TIMESTAMP_LTZ列上定义事件时间属性。
如果源中的时间戳数据表示为year-month-day-hour-minute-second，通常是没有时区信息的字符串值，
例如2020-04-1520:13:40.564，建议将事件时间属性定义为TIMESTAMP列：
```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- declare user_action_time as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
);
```

如果源中的时间戳数据表示为纪元时间，通常是一个长值，例如1618989564564，建议将事件时间属性定义为TIMESTAMP_LTZ列：
```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  ts BIGINT,
  time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  -- declare time_ltz as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND
)
```

### 数据流转换到表

将DataStream转换为表时，可以在模式定义期间使用. rowtime属性定义事件时间属性。
时间戳和水印必须已在正在转换的DataStream中分配。在转换期间，
Flink始终将rowtime属性派生为TIMESTAMP WITHOUT TIME ZONE，因为DataStream没有时区概念，并将所有事件时间值视为UTC。


将DataStream转换为Table时，有两种定义time属性的方法。
根据指定的.rowtime字段名称是否存在于DataStream的架构中，时间戳要么（1）作为新列附加，要么（2）替换现有列。


在任何一种情况下，事件时间时间戳字段都将保存DataStream事件时间戳的值。


```java
// Option 1:

// extract timestamp and assign watermarks based on knowledge of the stream
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// declare an additional logical field as an event time attribute
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").rowtime());


// Option 2:

// extract timestamp from first field, and assign watermarks based on knowledge of the stream
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// the first field has been used for timestamp extraction, and is no longer necessary
// replace first field with a logical event time attribute
Table table = tEnv.fromDataStream(stream, $("user_action_time").rowtime(), $("user_name"), $("data"));

// Usage:

WindowedTable windowedTable = table.window(Tumble
.over(lit(10).minutes())
.on($("user_action_time"))
.as("userActionWindow"));
```

package cn.mrt.flink.connector;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FileConnectorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1. 读取数据源
        tableEnv.executeSql("CREATE TABLE clickTableSource (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'src/main/resources/input/clicks.csv', " +
                " 'format' = 'csv' " +
                ")");
        Table resultTable = tableEnv.sqlQuery("select * from clickTableSource");
        // 4. 输出到Kafka Topic
        tableEnv.executeSql(("CREATE TABLE clickTableSink (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'src/main/resources/output/clicks', " +
                " 'format' = 'csv' " +
                ")"));
        //insert到哪张表
        resultTable.executeInsert("clickTableSink");
    }
}

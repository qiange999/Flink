package cn.mrt.flink.connector;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaConnectorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                DataStreamSource<Event> stream = env.addSource(new ClickSource());
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1. 读取数据源
        tableEnv.executeSql("CREATE TABLE KafkaTableSource (" +
                " `user` STRING," +
                " `url` STRING," +
                " `ts` TIMESTAMP(3) METADATA FROM 'timestamp'" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'events-source'," +
                " 'properties.bootstrap.servers' = '192.168.56.101:9092'," +
                " 'properties.group.id' = 'testGroup'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'format' = 'csv'" +
                ")");
        Table resultTable = tableEnv.sqlQuery("select * from KafkaTableSource");
        // 4. 输出到Kafka Topic
        tableEnv.executeSql("CREATE TABLE KafkaTableSink (" +
                " `user` STRING," +
                " `url` STRING," +
                " `ts` TIMESTAMP" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'events-sink'," +
                " 'properties.bootstrap.servers' = '192.168.56.101:9092'," +
                " 'value.format' = 'csv'" +
                ")");
        //insert到哪张表
        resultTable.executeInsert("KafkaTableSink");
    }
}

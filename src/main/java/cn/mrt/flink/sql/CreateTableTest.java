package cn.mrt.flink.sql;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CreateTableTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //创建表
        String createDDL = "CREATE TABLE clickTable(" +
                "user_name STRING," +
                "url STRING," +
                "ts BIGINT" +
                ") WITH (" +
                "'connector' = 'filesystem'," +
                "'path' = 'src/main/resources/input/clicks.csv', " +
                "'format' = 'csv' ) ";
        tableEnv.executeSql(createDDL);
        //查询表
        Table allTable = tableEnv.sqlQuery("select * from clickTable");
        Table userTable = tableEnv.sqlQuery("select user_name from clickTable");
        //注册表
        tableEnv.createTemporaryView("userTable",userTable);
        Table aliceTable = tableEnv.sqlQuery("select * from userTable where user_name = 'Alice'");
        //将表转化为数据流，打印输出
        tableEnv.toDataStream(allTable).print("all");
        tableEnv.toDataStream(userTable).print("user");
        tableEnv.toDataStream(aliceTable).print("Alice");
        env.execute();
    }
}

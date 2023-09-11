package cn.mrt.flink.sql;


import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableToStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                DataStreamSource<Event> stream = env.addSource(new ClickSource());
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
                tableEnv.createTemporaryView("eventTable",stream);
        Table eventTableResult = tableEnv.sqlQuery("select user,url from eventTable");
        Table eventTableResultSum = tableEnv.sqlQuery("select user,count(url) from eventTable GROUP BY user");
        tableEnv.toDataStream(eventTableResult).print("common");
        tableEnv.toChangelogStream(eventTableResultSum).print("change");
        env.execute();
    }
}

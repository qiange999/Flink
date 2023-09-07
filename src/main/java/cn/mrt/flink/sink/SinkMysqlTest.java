package cn.mrt.flink.sink;

import cn.mrt.flink.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkMysqlTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //添加数据
        DataStream<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L), new Event("Alice", "./prod?id=100", 3000L), new Event("Alice", "./prod?id=200", 3500L), new Event("Bob", "./prod?id=2", 2500L), new Event("Alice", "./prod?id=300", 3600L), new Event("Bob", "./home", 3000L), new Event("Bob", "./prod?id=1", 2300L), new Event("Bob", "./prod?id=3", 3300L));

        //sql
        String sql = "INSERT INTO clicks (user,url) VALUES(? , ?)";


        //指定event的数据字段存放在表格的哪个位置上
        JdbcStatementBuilder<Event> statementBuilder = new JdbcStatementBuilder<Event>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                String user = event.user;
                String url = event.url;
                preparedStatement.setString(1, user);
                preparedStatement.setString(2, url);
            }
        };


        //连接mysql的数据库参数
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://192.168.56.101:3306/flink")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("root123456")
                .build();
        //写入数据，填入参数
        SinkFunction<Event> jdbcSink = JdbcSink.sink(sql, statementBuilder, connectionOptions);
        stream.addSink(jdbcSink);
        env.execute();
    }
}

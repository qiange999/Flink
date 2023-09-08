package cn.mrt.flink.stream;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> mary = stream.filter(event -> event.user.equals("Mary"));
        SingleOutputStreamOperator<Event> bob = stream.filter(event -> event.user.equals("Bob"));
        mary.print("mary");
        bob.print("bob");

        mary.union(bob).print("union");

        env.execute();
    }
}

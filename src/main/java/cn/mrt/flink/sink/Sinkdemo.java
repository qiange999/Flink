package cn.mrt.flink.sink;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * @author  cqh
 * @version 1.0
 */
public class Sinkdemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
//        DataStream<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L), new Event("Alice", "./prod?id=100", 3000L), new Event("Alice", "./prod?id=200", 3500L), new Event("Bob", "./prod?id=2", 2500L), new Event("Alice", "./prod?id=300", 3600L), new Event("Bob", "./home", 3000L), new Event("Bob", "./prod?id=1", 2300L), new Event("Bob", "./prod?id=3", 3300L));
        SingleOutputStreamOperator<String> map = stream.map(new WritetoMySQLFunction());
        map.setParallelism(2);
        env.execute();
    }

    private static class WritetoMySQLFunction extends RichMapFunction<Event, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("打开mysql传输数据");
        }

        @Override
        public String map(Event event) throws Exception {
            int taskNum = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("线程" + taskNum + "正在传入的的数据的user" + event.user);
            return "";
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("关闭");
        }


    }
}
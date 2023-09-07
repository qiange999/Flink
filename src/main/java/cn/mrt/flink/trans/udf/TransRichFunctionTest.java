package cn.mrt.flink.trans.udf;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * @author  cqh
 * @version 1.0
 */
public class TransRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
//        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Smith", "./go", 3000L));
        streamSource.map(new RichMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("启动索引为:" + getRuntimeContext().getIndexOfThisSubtask() + "的任务开始");
            }

            @Override
            public Long map(Event event) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println("process = " + indexOfThisSubtask + "正在处理的数据：" + event.timestamp);
                return event.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }).print();
        env.execute();

    }
}





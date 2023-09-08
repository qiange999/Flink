package cn.mrt.flink.process;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFuntionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> watermarksStream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
        //美来一个元素就会调一次
        SingleOutputStreamOperator<String> process = watermarksStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
//                if (value.user == "Mary"){
//                    out.collect(value.user);
//                }else {
//                    out.collect(value.url);
//                }
//                System.out.println(ctx.timerService().currentWatermark());
//            }
                System.out.println(value.timestamp + " ctx : " + ctx.timestamp());
//        }).print();
            }
        });


        env.execute();

    }
}
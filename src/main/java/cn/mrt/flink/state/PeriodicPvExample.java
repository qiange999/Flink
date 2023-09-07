package cn.mrt.flink.state;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SingleOutputStreamOperator<Event> streamwithWatermark = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        //.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
        //统计每个用户的pv，隔一段时间（10s）输出一次结果
        SingleOutputStreamOperator<String> processStream = streamwithWatermark
                .keyBy(event -> event.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
            //定义两个状态
            ValueState<Long> countState;
            ValueState<Long> timerTsState;
            @Override
            public void open(Configuration parameters) throws Exception {
                //在open里才能获取到运行时状态，所以在成员变量里创建状态，在open里赋值
                 countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
                 timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                //通过open获取上下文状态
            }
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                //更新count值
                //在这里面注册定时器
                //解读：注意在这里知识用创建好的timerTsState，目的是每一条数据进来，都是公用同一个状态，这样就不会重复创建
                Long count = countState.value();
                if (count == null) {
                    countState.update(1L);
                } else {
                    countState.update(count + 1);
                }
                if (timerTsState.value() == null) {
                    ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                    timerTsState.update(value.timestamp + 10 * 1000L);//这个赋什么值都无所谓，不为空就行
                }
            }
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey() + " pv：" + countState.value());
                timerTsState.clear();
            }
        });
        processStream.print("input");
        env.execute();
    }
}

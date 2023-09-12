package cn.mrt.flink.function;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.Duration;

public class UdfTest_Scalarfunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                DataStreamSource<Event> stream = env.addSource(new ClickSource());
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<Event> streamOperator = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        //2 流转表并注册
        Table table = tableEnv.fromDataStream(streamOperator);
        tableEnv.createTemporaryView("eventTable",table);
        //3 注册自定义标量函数
        tableEnv.createTemporarySystemFunction("MyHash",MyHash.class);
        //4 调用sqlquery对字段做自定义操作
        Table sqlQuery = tableEnv.sqlQuery("select MyHash(user) as userHashcode,user,url from eventTable");
        tableEnv.toDataStream(sqlQuery).print();


        env.execute();
    }


    //自定义一个MyHash类
    public static class MyHash extends ScalarFunction {
        /*

        要求：
        1.方法必须public
        2.方法名称必须是eval
        3.必须要有一个返回值（自定义类型，看数据要求）
         */
         public int eval(String str){
             return str.hashCode();
         }
    }
}

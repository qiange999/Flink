package cn.mrt.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Properties;

/*
 * @author  cqh
 * @version 1.0
 */
public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //替代FlinkKafkaConsumer的官方方法，先设置一个KafkaSource。当前flink版本不适用。
//        KafkaSource<Object> source = KafkaSource.builder().setBootstrapServers("hadoop:9092")
//                .setTopics("clicks")
//                .setGroupId("kafka-flink")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new DeserializationSchema<>())
//                .build();
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), new SourceKafkaTest().getKafkaProperties()));
        source.print("kafka");
        //关键：kafka --> flink
//        DataStreamSource<Object> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
//        kafka_source.print();
        env.execute();
    }

    public Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "clicks");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return properties;
//    }

    }
}

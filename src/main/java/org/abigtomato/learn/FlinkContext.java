package org.abigtomato.learn;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 上下文
 *
 * @author abigtomato
 */
@Slf4j
public class FlinkContext {

    private final StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;

    public static FlinkContext init() {
        return new FlinkContext();
    }

    public static FlinkContext init(int parallelism) {
        return new FlinkContext(parallelism);
    }

    public FlinkContext() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public FlinkContext(int parallelism) {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(parallelism);
    }

    public StreamExecutionEnvironment env() {
        return this.env;
    }

    private SingleOutputStreamOperator<SensorReading> map(DataStream<String> inputStream) {
        return inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
    }

    public SingleOutputStreamOperator<String> getStreamDataFromText() {
        return this.getStreamDataFromText("D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\sensor.csv");
    }

    public SingleOutputStreamOperator<String> getStreamDataFromText(String path) {
        return this.env.readTextFile(path);
    }

    public SingleOutputStreamOperator<SensorReading> getStreamDataFromSocket() {
        return this.getStreamDataFromSocket("localhost", 7777);
    }

    public SingleOutputStreamOperator<SensorReading> getStreamDataFromSocket(String hostname, int port) {
        return this.map(this.env.socketTextStream(hostname, port));
    }

    public SingleOutputStreamOperator<String> getStreamDataFromKafka() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.105:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        return this.getStreamDataFromKafka(properties);
    }

    public SingleOutputStreamOperator<String> getStreamDataFromKafka(Properties properties) {
        return this.env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
    }

    public DataStreamSink<String> addStreamDataToKafka(DataStream<String> dataStream) {
        return dataStream.addSink(new FlinkKafkaProducer<>("localhost:9092", "sinkTest", new SimpleStringSchema()));
    }

    public void kafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 1);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first", String.valueOf(i), String.valueOf(i)), (recordMetadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println("recordMetadata = " + recordMetadata.toString());
                }
            });
        }
    }

    public void execute() {
        try {
            this.env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

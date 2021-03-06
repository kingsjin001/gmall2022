package com.djin.gmallrealtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author dj
 */
public class KafkaUtil {
    private static final String KAFKA_SERVER = "node001:9092,node002:9092,node003:9092";
    private static final Properties properties = new Properties();
    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }
    /**
     * 获取KafkaSink的方法
     * @param topic  主题
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }
    /**
     * 获取KafkaSource的方法
     * @param topic  主题
     * @param groupId   消费者组
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        //给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }
}

package org.aidarko.tools;


import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;

public class KafkaMguentherTopicReader {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "my.topic.name";

    public static void main(String[] args) throws InterruptedException {
        List<KeyValue<String, String>> keyValues = readAll(TOPIC);
        System.out.println(keyValues);
    }

    private static List<KeyValue<String, String>> readAll(String topic) throws InterruptedException {
        DefaultRecordConsumer defaultRecordConsumer = new DefaultRecordConsumer(BOOTSTRAP_SERVERS);
        return defaultRecordConsumer.read(
                ReadKeyValues.from(topic, String.class, String.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id-1")
                        .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        );
    }
}

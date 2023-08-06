package com.aidarko.table.serde;

import com.aidarko.table.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class UserDeserializationSchema implements KafkaRecordDeserializationSchema<User>, Deserializer<User> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<User> out) {
        User user = deserialize("", record.value());
        if (user != null) {
            out.collect(user);
        }
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(User.class);
    }

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            log.debug("Deserializing User");
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), User.class);
        } catch (IOException e) {
            log.error("Error when deserializing User from byte[]");
            return null;
        }
    }
}

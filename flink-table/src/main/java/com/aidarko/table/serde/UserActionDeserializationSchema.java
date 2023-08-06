package com.aidarko.table.serde;

import com.aidarko.table.model.UserAction;
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
public class UserActionDeserializationSchema implements KafkaRecordDeserializationSchema<UserAction>, Deserializer<UserAction> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UserAction> out) {
        UserAction userAction = deserialize("", record.value());
        if (userAction != null) {
            out.collect(userAction);
        }
    }

    @Override
    public TypeInformation<UserAction> getProducedType() {
        return TypeInformation.of(UserAction.class);
    }

    @Override
    public UserAction deserialize(String topic, byte[] data) {
        try {
            log.debug("Deserializing UserAction");
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), UserAction.class);
        } catch (IOException e) {
            log.error("Error when deserializing UserAction from byte[]", e);
            return null;
        }
    }
}

package com.aidarko.simple.serde;

import com.aidarko.simple.model.Action;
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
public class ActionDeserializationSchema implements KafkaRecordDeserializationSchema<Action>, Deserializer<Action> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TypeInformation<Action> getProducedType() {
        return TypeInformation.of(Action.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Action> out) {
        Action action = deserialize("", record.value());
        if (action != null) {
            out.collect(action);
        }
    }

    @Override
    public Action deserialize(String topic, byte[] data) {
        try {
            log.debug("Deserializing Action");
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), Action.class);
        } catch (IOException e) {
            log.error("Error when deserializing Action from byte[]");
            return null;
        }
    }
}

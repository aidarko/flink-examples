package com.aidarko.broadcast.serde;

import com.aidarko.broadcast.model.Rule;
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
public class RuleDeserializationSchema implements KafkaRecordDeserializationSchema<Rule>, Deserializer<Rule> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Rule> out) {
        Rule rule = deserialize("", record.value());
        if (rule != null) {
            out.collect(rule);
        }
    }

    @Override
    public TypeInformation<Rule> getProducedType() {
        return TypeInformation.of(Rule.class);
    }

    @Override
    public Rule deserialize(String topic, byte[] data) {
        try {
            log.debug("Deserializing Rule");
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), Rule.class);
        } catch (IOException e) {
            log.error("Error when deserializing Rule from byte[]");
            return null;
        }
    }
}

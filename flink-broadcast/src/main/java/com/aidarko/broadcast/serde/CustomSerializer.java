package com.aidarko.broadcast.serde;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationException;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class CustomSerializer<T> implements Serializer<T>, SerializationSchema<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        return serialize(data);
    }

    @Override
    public byte[] serialize(T data) {
        if (data == null) {
            log.warn("Null received at serializing");
            return null;
        }
        try {
            log.info("Serializing {} to byte[]...", data);
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            log.error("Error when serializing {} to byte[]", data);
            throw new SerializationException("Error when serializing to byte[]");
        }
    }
}

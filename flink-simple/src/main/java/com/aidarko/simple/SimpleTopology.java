package com.aidarko.simple;

import com.aidarko.simple.function.EnrichmentFunction;
import com.aidarko.simple.model.Action;
import com.aidarko.simple.model.User;
import com.aidarko.simple.model.UserAction;
import com.aidarko.simple.serde.ActionDeserializationSchema;
import com.aidarko.simple.serde.CustomSerializer;
import com.aidarko.simple.serde.UserDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleTopology {

    public static final String USER_TOPIC = "com.aidarko.model.user";
    public static final String ACTION_TOPIC = "com.aidarko.model.action";
    public static final String USER_ACTION_TOPIC = "com.aidarko.model.user.action";

    private final String bootstrapServers;
    private final StreamExecutionEnvironment env;

    public SimpleTopology(StreamExecutionEnvironment env, String bootstrapServers) {
        this.env = env;
        this.bootstrapServers = bootstrapServers;
    }

    public void topology() {
        KeyedStream<User, String> userKeyedStream = env.fromSource(userSourceStream(), WatermarkStrategy.noWatermarks(), "user stream")
                .keyBy(User::getUserId);

        KeyedStream<Action, String> actionKeyedStream = env.fromSource(actionSourceStream(), WatermarkStrategy.noWatermarks(), "action stream")
                .keyBy(Action::getUserId);

        userActionDataStream(userKeyedStream, actionKeyedStream)
                .sinkTo(userActionSink())
                .name("merged user with action");
    }

    static DataStream<UserAction> userActionDataStream(
            KeyedStream<User, String> userKeyedStream,
            KeyedStream<Action, String> actionKeyedStream
    ) {
        return userKeyedStream
                .connect(actionKeyedStream)
                .flatMap(new EnrichmentFunction());
    }

    private KafkaSource<User> userSourceStream() {
        return KafkaSource.<User>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(USER_TOPIC)
                .setGroupId("my-user-group")
                .setDeserializer(new UserDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

    private KafkaSource<Action> actionSourceStream() {
        return KafkaSource.<Action>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(ACTION_TOPIC)
                .setGroupId("my-action-group")
                .setDeserializer(new ActionDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

    private KafkaSink<UserAction> userActionSink() {
        return KafkaSink.<UserAction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(USER_ACTION_TOPIC)
                                .setKeySerializationSchema(new CustomSerializer<>())
                                .setValueSerializationSchema(new CustomSerializer<UserAction>())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}

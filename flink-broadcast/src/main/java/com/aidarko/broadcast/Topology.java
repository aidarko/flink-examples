package com.aidarko.broadcast;

import com.aidarko.broadcast.function.RuleBroadCastFn;
import com.aidarko.broadcast.model.Action;
import com.aidarko.broadcast.model.Rule;
import com.aidarko.broadcast.serde.ActionDeserializationSchema;
import com.aidarko.broadcast.serde.CustomSerializer;
import com.aidarko.broadcast.serde.RuleDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Topology {

    public static final String RULE_TOPIC = "com.aidarko.model.rule";
    public static final String ACTION_TOPIC = "com.aidarko.model.action";
    public static final String FILTERED_ACTION_TOPIC = "com.aidarko.model.action.filtered";

    private final String bootstrapServers;
    private final StreamExecutionEnvironment env;

    public Topology(StreamExecutionEnvironment env, String bootstrapServers) {
        this.env = env;
        this.bootstrapServers = bootstrapServers;
    }

    public void topology() {
        // rule source stream
        DataStream<Rule> ruleStream = env.fromSource(ruleSourceStream(), WatermarkStrategy.noWatermarks(), "rule stream");

        // action source stream
        KeyedStream<Action, String> actionKeyedStream = env.fromSource(actionSourceStream(), WatermarkStrategy.noWatermarks(), "action stream")
                .keyBy(Action::getAction);

        filteredActionStream(ruleStream, actionKeyedStream)
                .sinkTo(actionSink())
                .name("filtered actions");
    }

    static SingleOutputStreamOperator<Action> filteredActionStream(DataStream<Rule> ruleStream, KeyedStream<Action, String> actionKeyedStream) {
        BroadcastStream<Rule> broadcast = ruleStream.broadcast(RuleBroadCastFn.ruleStateDescriptor);

        // filtered action stream
        return actionKeyedStream
                .connect(broadcast)
                .process(new RuleBroadCastFn());

    }

    private KafkaSource<Rule> ruleSourceStream() {
        return KafkaSource.<Rule>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(RULE_TOPIC)
                .setGroupId("my-rule-group")
                .setDeserializer(new RuleDeserializationSchema())
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

    private KafkaSink<Action> actionSink() {
        return KafkaSink.<Action>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(FILTERED_ACTION_TOPIC)
                                .setKeySerializationSchema(new CustomSerializer<>())
                                .setValueSerializationSchema(new CustomSerializer<Action>())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}

package com.aidarko.broadcast;

import com.aidarko.broadcast.model.Action;
import com.aidarko.broadcast.model.Rule;
import com.aidarko.broadcast.serde.ActionDeserializationSchema;
import com.aidarko.broadcast.serde.CustomSerializer;
import com.aidarko.broadcast.serde.RuleDeserializationSchema;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.aidarko.broadcast.Topology.ACTION_TOPIC;
import static com.aidarko.broadcast.Topology.FILTERED_ACTION_TOPIC;
import static com.aidarko.broadcast.Topology.RULE_TOPIC;
import static java.lang.Thread.sleep;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaSmokeBroadcastTest {
    private final Properties localProducerProperties = localProducerProperties();
    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setup() throws Exception {
        setupKafka();
        setupFlink();
    }

    private void setupKafka() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();

        kafka.createTopic(TopicConfig.withName(RULE_TOPIC).build());
        kafka.createTopic(TopicConfig.withName(ACTION_TOPIC).build());
    }

    private void setupFlink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        new Topology(env, kafka.getBrokerList()).topology();
        env.executeAsync("Filtered Action data pipeline");
    }

    private static Properties localProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("client.id", "Filtered Action client");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class);
        return properties;
    }

    @Test
    @DisplayName("When Rule and Action are published to Kafka, then Action is filtered")
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void test_kafka_flink() throws InterruptedException {
        // given
        ArrayList<String> allowedActions = new ArrayList<>();
        allowedActions.add("logged-in");
        Rule rule = new Rule("id1", allowedActions);
        Action action = new Action(UUID.randomUUID().toString(), "id1", "logged-in");

        sendRule(rule);
        sleep(100);
        sendAction(action);

        // when
        // ... async execution
        kafka.observe(on(FILTERED_ACTION_TOPIC, 1).observeFor(1, TimeUnit.MINUTES));

        // then
        List<KeyValue<String, Rule>> rules = readRules();
        List<KeyValue<String, Action>> actions = readActions();
        List<KeyValue<String, Action>> filteredActions = readFilteredActions();

        assertEquals(1, rules.size());
        assertEquals(1, actions.size());
        assertEquals(1, filteredActions.size());

        assertEquals(rule, rules.get(0).getValue());
        assertEquals(action, actions.get(0).getValue());

        Action actualAction = filteredActions.get(0).getValue();
        assertEquals(action, actualAction);
    }

    private void sendAction(Action action) throws InterruptedException {
        try (Producer<String, Action> producer = new KafkaProducer<>(localProducerProperties)) {
            producer.send(new ProducerRecord<>(ACTION_TOPIC, action.getActionId(), action));
        }
        kafka.observe(on(ACTION_TOPIC, 1));
    }

    private void sendRule(Rule rule) throws InterruptedException {
        try (Producer<String, Rule> producer = new KafkaProducer<>(localProducerProperties)) {
            producer.send(new ProducerRecord<>(RULE_TOPIC, rule.getRuleId(), rule));
        }
        kafka.observe(on(RULE_TOPIC, 1));
    }

    private List<KeyValue<String, Action>> readFilteredActions() throws InterruptedException {
        return kafka.read(
                ReadKeyValues.from(FILTERED_ACTION_TOPIC, String.class, Action.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ActionDeserializationSchema.class)
        );
    }

    private List<KeyValue<String, Action>> readActions() throws InterruptedException {
        return kafka.read(
                ReadKeyValues.from(ACTION_TOPIC, String.class, Action.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ActionDeserializationSchema.class)
        );
    }

    private List<KeyValue<String, Rule>> readRules() throws InterruptedException {
        return kafka.read(
                ReadKeyValues.from(RULE_TOPIC, String.class, Rule.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RuleDeserializationSchema.class)
        );
    }
}

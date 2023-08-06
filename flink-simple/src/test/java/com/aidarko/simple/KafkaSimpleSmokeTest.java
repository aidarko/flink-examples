package com.aidarko.simple;

import com.aidarko.simple.model.Action;
import com.aidarko.simple.model.User;
import com.aidarko.simple.model.UserAction;
import com.aidarko.simple.serde.ActionDeserializationSchema;
import com.aidarko.simple.serde.CustomSerializer;
import com.aidarko.simple.serde.UserActionDeserializationSchema;
import com.aidarko.simple.serde.UserDeserializationSchema;
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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.aidarko.simple.SimpleTopology.ACTION_TOPIC;
import static com.aidarko.simple.SimpleTopology.USER_ACTION_TOPIC;
import static com.aidarko.simple.SimpleTopology.USER_TOPIC;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaSimpleSmokeTest {
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

        kafka.createTopic(TopicConfig.withName(USER_TOPIC).build());
        kafka.createTopic(TopicConfig.withName(ACTION_TOPIC).build());
    }

    private void setupFlink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        new SimpleTopology(env, kafka.getBrokerList()).topology();
        env.executeAsync("User-Action data pipeline");
    }

    private static Properties localProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("client.id", "User-Action client");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class);
        return properties;
    }

    @Test
    @DisplayName("When User and Action are published to Kafka, then UserAction is published to Kafka")
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void test_kafka_flink() throws InterruptedException {
        // given
        User user = new User("id1", "name1");
        Action action = new Action("id1", "logged-in");

        sendUser(user);
        sendAction(action);

        // when
        // ... async execution
        kafka.observe(on(USER_ACTION_TOPIC, 1).observeFor(1, TimeUnit.MINUTES));

        // then
        List<KeyValue<String, User>> users = readUsers();
        List<KeyValue<String, Action>> actions = readActions();
        List<KeyValue<String, UserAction>> userActions = readUserActions();

        assertEquals(1, users.size());
        assertEquals(1, actions.size());
        assertEquals(1, userActions.size());

        assertEquals(user, users.get(0).getValue());
        assertEquals(action, actions.get(0).getValue());

        UserAction actualUserAction = userActions.get(0).getValue();
        assertEquals(action.getAction(), actualUserAction.getAction());
        assertEquals(user.getUserId(), actualUserAction.getUserId());
        assertEquals(user.getName(), actualUserAction.getName());
    }

    private void sendAction(Action action) throws InterruptedException {
        try (Producer<String, Action> producer = new KafkaProducer<>(localProducerProperties)) {
            producer.send(new ProducerRecord<>(ACTION_TOPIC, action.getUserId(), action));
        }
        kafka.observe(on(ACTION_TOPIC, 1));
    }

    private void sendUser(User user) throws InterruptedException {
        try (Producer<String, User> producer = new KafkaProducer<>(localProducerProperties)) {
            producer.send(new ProducerRecord<>(USER_TOPIC, user.getUserId(), user));
        }
        kafka.observe(on(USER_TOPIC, 1));
    }

    private List<KeyValue<String, UserAction>> readUserActions() throws InterruptedException {
        return kafka.read(
                ReadKeyValues.from(USER_ACTION_TOPIC, String.class, UserAction.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserActionDeserializationSchema.class)
        );
    }

    private List<KeyValue<String, Action>> readActions() throws InterruptedException {
        return kafka.read(
                ReadKeyValues.from(ACTION_TOPIC, String.class, Action.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ActionDeserializationSchema.class)
        );
    }

    private List<KeyValue<String, User>> readUsers() throws InterruptedException {
        return kafka.read(
                ReadKeyValues.from(USER_TOPIC, String.class, User.class)
                        .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializationSchema.class)
        );
    }
}

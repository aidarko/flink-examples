package com.aidarko.table;

import com.aidarko.table.model.Action;
import com.aidarko.table.model.User;
import com.aidarko.table.model.UserAction;
import com.aidarko.table.serde.ActionDeserializationSchema;
import com.aidarko.table.serde.CustomSerializer;
import com.aidarko.table.serde.UserDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class Topology {

    public static final String USER_TOPIC = "com.aidarko.model.user";
    public static final String ACTION_TOPIC = "com.aidarko.model.action";
    public static final String USER_ACTION_TOPIC = "com.aidarko.model.user.action";

    private final String bootstrapServers;
    private final StreamExecutionEnvironment env;

    public Topology(StreamExecutionEnvironment env, String bootstrapServers) {
        this.env = env;
        this.bootstrapServers = bootstrapServers;
    }

    public void topology() {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create data streams
        KeyedStream<User, String> userKeyedStream = env.fromSource(userSourceStream(), WatermarkStrategy.noWatermarks(), "user stream")
                .keyBy(User::getUserId);
        KeyedStream<Action, String> actionKeyedStream = env.fromSource(actionSourceStream(), WatermarkStrategy.noWatermarks(), "action stream")
                .keyBy(Action::getUserId);

        // userActionDataStream from tableEnv
        DataStream<UserAction> userActionStreamFromTable = userActionDataStream(tableEnv, userKeyedStream, actionKeyedStream);

        // sink
        userActionStreamFromTable
                .sinkTo(userActionSink())
                .name("merged user with action");
    }

    static DataStream<UserAction> userActionDataStream(StreamTableEnvironment tableEnv, KeyedStream<User, String> userKeyedStream, KeyedStream<Action, String> actionKeyedStream) {
        // create tables and join by userId
        Table userTable = createUserTable(userKeyedStream, tableEnv);
        Table actionTable = createActionTable(actionKeyedStream, tableEnv);
        Table joinedTable =
                tableEnv.sqlQuery(
                        "SELECT U.userId, U.name, A.action " +
                                "FROM UserTable U, ActionTable A " +
                                "WHERE U.userId = A.userId");

        // raw output
        DataStream<Row> userStreamFromTable = tableEnv.toDataStream(userTable);
        userStreamFromTable.print();

        // raw output
        DataStream<Row> actionStreamFromTable = tableEnv.toDataStream(actionTable);
        actionStreamFromTable.print();

        // pojo output
        DataStream<UserAction> userActionStreamFromTable = tableEnv.toDataStream(joinedTable, UserAction.class);
        userActionStreamFromTable.print();
        return userActionStreamFromTable;
    }

    private static Table createUserTable(KeyedStream<User, String> userKeyedStream, StreamTableEnvironment tableEnv) {
        Table inputTable = tableEnv.fromDataStream(userKeyedStream);
        log.info("ResolvedSchema for userKeyedStream:\n{}", inputTable.getResolvedSchema().toString());

        tableEnv.createTemporaryView("UserTable", inputTable);

        return tableEnv.sqlQuery("SELECT userId, name FROM UserTable");
    }

    private static Table createActionTable(KeyedStream<Action, String> actionKeyedStream, StreamTableEnvironment tableEnv) {
        Table inputTable = tableEnv.fromDataStream(actionKeyedStream);
        log.info("ResolvedSchema for actionKeyedStream:\n{}", inputTable.getResolvedSchema().toString());

        tableEnv.createTemporaryView("ActionTable", inputTable);

        return tableEnv.sqlQuery("SELECT userId, action FROM ActionTable");
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

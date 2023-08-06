package com.aidarko.table;

import com.aidarko.table.model.Action;
import com.aidarko.table.model.User;
import com.aidarko.table.model.UserAction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyTest {

    @Test
    void userActionDataStreamTest() throws Exception {
        // prepare data
        User user = new User("userId1", "name1");
        Action action = new Action("userId1", "logged-in");
        UserAction userAction = new UserAction("userId1", "name1", "logged-in");

        // setup local Flink
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()) {
            env.setParallelism(2);

            KeyedStream<User, String> userDataStreamSource = env.fromElements(user).keyBy(User::getUserId);
            KeyedStream<Action, String> actionDataStreamSource = env.fromElements(action).keyBy(Action::getUserId);

            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            // create stream
            Topology.userActionDataStream(tableEnv, userDataStreamSource, actionDataStreamSource)
                    .addSink(new CollectSink());

            env.execute();
        }

        // verify
        assertEquals(1, CollectSink.values.size());
        assertTrue(CollectSink.values.contains(userAction));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<UserAction> {

        // must be static
        public static final List<UserAction> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(UserAction value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
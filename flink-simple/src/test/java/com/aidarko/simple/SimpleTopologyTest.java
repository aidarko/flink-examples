package com.aidarko.simple;

import com.aidarko.simple.model.Action;
import com.aidarko.simple.model.User;
import com.aidarko.simple.model.UserAction;
import com.aidarko.simple.SimpleTopology;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleTopologyTest {

    @Test
    void userActionDataStreamTest() throws Exception {
        // prepare data
        User user = new User("id1", "name1");
        Action action = new Action("id1", "logged-in");
        UserAction userAction = new UserAction("id1", "name1", "logged-in");

        // setup local Flink
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()) {
            env.setParallelism(2);

            KeyedStream<User, String> userDataStreamSource = env.fromElements(user).keyBy(User::getUserId);
            KeyedStream<Action, String> actionDataStreamSource = env.fromElements(action).keyBy(Action::getUserId);

            // create stream
            SimpleTopology.userActionDataStream(userDataStreamSource, actionDataStreamSource)
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
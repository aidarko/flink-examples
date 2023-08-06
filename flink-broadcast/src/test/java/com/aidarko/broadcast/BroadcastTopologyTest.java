package com.aidarko.broadcast;

import com.aidarko.broadcast.model.Action;
import com.aidarko.broadcast.model.Rule;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RichSourceFunction is deprecated and add a lot of mess by requiring to add `.returns(Rule.class)` or make Action and
 * Rule implement Serializable.
 */
class BroadcastTopologyTest {

    private static final AtomicInteger current = new AtomicInteger(0);

    @Test
    void ruleActionDataStreamTest() throws Exception {
        // prepare data
        ArrayList<String> allowedActions = new ArrayList<>();
        allowedActions.add("logged-in");
        Rule rule = new Rule("id1", allowedActions);
        Action action = new Action(UUID.randomUUID().toString(), "id1", "logged-in");

        // setup local Flink
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()) {
            env.setParallelism(2);

            DataStream<Rule> ruleDataStreamSource = env.addSource(new ControllableFunction<>(rule, 0))
                    .returns(Rule.class);

            KeyedStream<Action, String> actionDataStreamSource = env.addSource(new ControllableFunction<>(action, 1))
                    .returns(Action.class)
                    .keyBy(Action::getUserId);

            // create stream
            Topology.filteredActionStream(ruleDataStreamSource, actionDataStreamSource)
                    .addSink(new CollectSink());

            env.execute();
        }

        // verify
        assertEquals(1, CollectSink.values.size());
        assertEquals(action, CollectSink.values.get(0));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Action> {

        // must be static
        public static final List<Action> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Action value, SinkFunction.Context context) {
            values.add(value);
        }
    }

    static class ControllableFunction<T> extends RichSourceFunction<T> {
        private transient volatile boolean isRunning;
        private final T element;
        private final int order;

        ControllableFunction(T element, int order) {
            this.element = element;
            this.order = order;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            isRunning = true;
        }

        @Override
        public void run(SourceContext<T> ctx) {
            while (isRunning) {
                if (current.get() == order) {
                    ctx.collect(element);
                    current.incrementAndGet();
                    isRunning = false;
                } else {
                    ctx.markAsTemporarilyIdle();
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
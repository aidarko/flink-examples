package com.aidarko.simple.function;


import com.aidarko.simple.model.Action;
import com.aidarko.simple.model.User;
import com.aidarko.simple.model.UserAction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichmentFunction extends RichCoFlatMapFunction<User, Action, UserAction> {

    private ValueState<User> userState;
    private ValueState<Action> actionState;

    @Override
    public void open(Configuration config) {
        userState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("saved user", User.class));
        actionState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("saved action", Action.class));
    }

    @Override
    public void flatMap1(User user, Collector<UserAction> out) throws Exception {

        Action action = actionState.value();
        if (action != null) {
            actionState.clear();
            out.collect(new UserAction(user.getUserId(), user.getName(), action.getAction()));
        } else {
            userState.update(user);
        }
    }

    @Override
    public void flatMap2(Action action, Collector<UserAction> out) throws Exception {
        User user = userState.value();
        if (user != null) {
            // In that user case User should be kept
            // userState.clear();
            userState.update(user);
            out.collect(new UserAction(user.getUserId(), user.getName(), action.getAction()));
        } else {
            actionState.update(action);
        }
    }
}
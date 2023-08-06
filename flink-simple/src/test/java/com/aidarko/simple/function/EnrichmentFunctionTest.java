package com.aidarko.simple.function;


import com.aidarko.simple.function.EnrichmentFunction;
import com.aidarko.simple.model.Action;
import com.aidarko.simple.model.User;
import com.aidarko.simple.model.UserAction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnrichmentFunctionTest {

    private TwoInputStreamOperatorTestHarness<User, Action, UserAction> testHarness;
    private final EnrichmentFunction enrichmentFunction = new EnrichmentFunction();

    @BeforeEach
    void setup() throws Exception {
        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new CoStreamFlatMap<>(enrichmentFunction),
                User::getUserId,
                Action::getUserId,
                Types.STRING
        );
        testHarness.open();
    }

    @Test
    @DisplayName("When User and Action matched then UserAction created")
    public void testUserAndActionMatch() throws Exception {
        // given
        User user = new User("id1", "name1");
        Action action = new Action("id1", "logged-in");

        // when
        testHarness.processElement1(user, 10);
        testHarness.processElement2(action, 10);

        // then
        List<UserAction> userActions = testHarness.extractOutputValues();

        UserAction actualUserAction = userActions.get(0);
        assertEquals(1, userActions.size());
        assertEquals(action.getAction(), actualUserAction.getAction());
        assertEquals(user.getUserId(), actualUserAction.getUserId());
        assertEquals(user.getName(), actualUserAction.getName());
    }

    @Test
    public void testClearingValueState() throws Exception {
        // given data
        User user = new User("id1", "name1");
        Action action = new Action("id1", "logged-in");
        UserAction userAction = new UserAction("id1", "name1", "logged-in");

        // send first user record
        testHarness.processElement1(user, 10);
        ValueState<User> userPreviousInput =
                enrichmentFunction.getRuntimeContext().getState(
                        new ValueStateDescriptor<>("saved user", User.class));
        User stateValue = userPreviousInput.value();
        Assertions.assertEquals(Lists.newArrayList(), testHarness.extractOutputStreamRecords());
        Assertions.assertEquals(user, stateValue);

        // send second action record
        testHarness.processElement2(action, 20);
        ValueState<Action> actionPreviousInput =
                enrichmentFunction.getRuntimeContext().getState(
                        new ValueStateDescriptor<>("saved action", Action.class));
        // new record produced
        Assertions.assertEquals(
                Lists.newArrayList(new StreamRecord<>(userAction, 20)),
                testHarness.extractOutputStreamRecords());
        // user is kept
        Assertions.assertEquals(user, userPreviousInput.value());
        // action is gone
        Assertions.assertNull(actionPreviousInput.value());
    }
}
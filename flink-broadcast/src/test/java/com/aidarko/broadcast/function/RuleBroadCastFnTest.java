package com.aidarko.broadcast.function;

import com.aidarko.broadcast.model.Action;
import com.aidarko.broadcast.model.Rule;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleBroadCastFnTest {
    private RuleBroadCastFn processFunction;
    private KeyedBroadcastOperatorTestHarness<String, Action, Rule, Action> testHarness;

    @BeforeEach
    void setup() throws Exception {
        processFunction = new RuleBroadCastFn();
        testHarness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
                processFunction,
                (KeySelector<Action, String>) Action::getActionId,
                Types.STRING,
                RuleBroadCastFn.ruleStateDescriptor
        );
        testHarness.setup();
        testHarness.open();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    void pass_action_test() throws Exception {
        // given
        ArrayList<String> allowedActions = new ArrayList<>();
        allowedActions.add("logging-in");
        Rule rule = new Rule("ruleId", allowedActions);
        Action action = new Action("actionId", "userId", "logging-in");

        // when
        testHarness.processBroadcastElement(rule, 10);
        testHarness.processElement(action, 20);

        // then
        List<Action> actions = testHarness.extractOutputValues();
        assertEquals(1, actions.size());
        assertEquals(action, actions.get(0));
    }

    @Test
    void filter_out_action_test() throws Exception {
        // given
        ArrayList<String> allowedActions = new ArrayList<>();
        allowedActions.add("logging-out");
        Rule rule = new Rule("ruleId", allowedActions);
        Action action = new Action("actionId", "userId", "logging-in");

        // when
        testHarness.processBroadcastElement(rule, 10);
        testHarness.processElement(action, 20);

        // then
        List<Action> actions = testHarness.extractOutputValues();
        assertTrue(actions.isEmpty());
    }
}
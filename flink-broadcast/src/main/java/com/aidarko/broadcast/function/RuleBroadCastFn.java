package com.aidarko.broadcast.function;

import com.aidarko.broadcast.model.Action;
import com.aidarko.broadcast.model.Rule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class RuleBroadCastFn extends KeyedBroadcastProcessFunction<String, Action, Rule, Action> {
    public static final MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
            "ruleState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<>() {
            })
    );

    @Override
    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Rule, Action>.ReadOnlyContext ctx, Collector<Action> out) throws Exception {
        log.debug("Processing element {}", value);
        Rule rule = ctx.getBroadcastState(ruleStateDescriptor).get(null);
        if (rule != null && rule.getAllowedActions().contains(value.getAction())) {
            out.collect(value);
        }
    }

    @Override
    public void processBroadcastElement(Rule value, KeyedBroadcastProcessFunction<String, Action, Rule, Action>.Context ctx, Collector<Action> out) throws Exception {
        log.debug("Adding to broadcast ruleState {}", value);
        // put the received Rule record in to the broadcast state using the null key (we only store a single rule in the MapState)
        ctx.getBroadcastState(ruleStateDescriptor).put(null, value);
    }
}

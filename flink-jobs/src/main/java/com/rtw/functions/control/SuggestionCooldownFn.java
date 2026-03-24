package com.rtw.functions.control;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 轻量节流：同一个 key（建议你 keyBy run_id|target）在 cooldownMs 内只放行一次
 * - 状态 TTL 防止 key 无限增长
 */
public class SuggestionCooldownFn extends KeyedProcessFunction<String, String, String> {

    private final long cooldownMs;

    private transient ValueState<Long> lastEmitMs;

    public SuggestionCooldownFn(long cooldownMs) {
        this.cooldownMs = cooldownMs;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("lastEmitMs", Long.class);

        // TTL：避免状态无限增长（你可以按 run 时长调）
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.hours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        desc.enableTimeToLive(ttl);

        lastEmitMs = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        long now = System.currentTimeMillis();
        Long last = lastEmitMs.value();

        if (last != null && (now - last) < cooldownMs) {
            return;
        }
        lastEmitMs.update(now);
        out.collect(value);
    }
}

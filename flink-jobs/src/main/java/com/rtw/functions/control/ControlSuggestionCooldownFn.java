package com.rtw.functions.control;

import com.rtw.model.ControlSuggestion;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 控制建议冷却（节流）：
 * - keyBy: run_id|target
 * - cooldownMs 内只放行一次（避免连续帧碰撞刷爆 Kafka topic）
 *
 * 说明：
 * - 用 ValueState 存最后一次放行时间
 * - 加 TTL 防止 key 无限增长
 */
public class ControlSuggestionCooldownFn extends KeyedProcessFunction<String, ControlSuggestion, ControlSuggestion> {

    private final long cooldownMs;
    private transient ValueState<Long> lastEmitMs;

    public ControlSuggestionCooldownFn(long cooldownMs) {
        this.cooldownMs = cooldownMs;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("lastEmitMs", Long.class);

        // TTL：1 小时（按你的 run 时长可调）
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.hours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        desc.enableTimeToLive(ttl);

        lastEmitMs = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(ControlSuggestion value, Context ctx, Collector<ControlSuggestion> out) throws Exception {
        if (value == null) return;

        long now = System.currentTimeMillis();
        Long last = lastEmitMs.value();

        if (last != null && (now - last) < cooldownMs) {
            return; // drop
        }
        lastEmitMs.update(now);
        out.collect(value);
    }
}

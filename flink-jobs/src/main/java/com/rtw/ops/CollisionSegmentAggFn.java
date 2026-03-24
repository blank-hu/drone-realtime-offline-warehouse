package com.rtw.ops;

import com.rtw.config.AppConfig;
import com.rtw.model.CollisionHit;
import com.rtw.model.DwsCollisionEvent;
import com.rtw.model.OdsTrajPoint;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 碰撞 segment 聚合：
 *   CollisionHit（逐帧命中） -> segment 合并 -> run_end_marker(event-time timer) flush -> Doris UNIQUE 覆盖更新
 *
 * 推荐 keyBy：run_id
 * - 好处：run_end_marker 到来时，可以一次性 flush 当前 run 的所有 pair 段状态（MapState 可遍历）
 *
 * 段连续条件（严格帧连续）：
 *   hit.frame_t_ms == last_frame_t_ms + frameMs
 *
 * gap 切段：
 *   hit.frame_t_ms - last_frame_t_ms > maxGapMs（默认 2 帧）
 */
public class CollisionSegmentAggFn
        extends KeyedCoProcessFunction<String, CollisionHit, OdsTrajPoint, DwsCollisionEvent> {

    private static final long IDLE_FLUSH_MS = 15000; // 兜底：长时间无输入时 flush+clear（防泄漏）

    private final AppConfig cfg;
    private final long frameMs;
    private final long maxGapMs;

    // run_id 内：pairKey -> open segment
    private transient MapState<String, CollisionSegState> openSegByPair;

    // event-time 定稿：timer(end_ms)
    private transient ValueState<Long> finalizeTimerTs;
    private transient ValueState<Boolean> finalized;

    // processing-time 兜底 idle flush
    private transient ValueState<Long> lastSeenProcTs;
    private transient ValueState<Long> idleTimerTs;

    public CollisionSegmentAggFn(AppConfig cfg) {
        this.cfg = cfg;
        this.frameMs = Math.max(1L, cfg.trajFrameMs); // 40ms
        this.maxGapMs = Math.max(frameMs, frameMs * 2L); // 允许最多 2 帧间隙
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        MapStateDescriptor<String, CollisionSegState> desc =
                new MapStateDescriptor<>("collOpenSeg",
                        Types.STRING,
                        Types.POJO(CollisionSegState.class));
        openSegByPair = getRuntimeContext().getMapState(desc);

        finalizeTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("collFinalizeTs", Long.class));
        finalized = getRuntimeContext().getState(new ValueStateDescriptor<>("collFinalized", Boolean.class));

        lastSeenProcTs = getRuntimeContext().getState(new ValueStateDescriptor<>("collLastSeenProc", Long.class));
        idleTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("collIdleTimer", Long.class));
    }

    // ============= 输入1：CollisionHit（帧级命中）=============
    @Override
    public void processElement1(CollisionHit hit, Context ctx, Collector<DwsCollisionEvent> out) throws Exception {
        if (isFinalized()) return;
        if (hit == null || hit.run_id == null || hit.run_id.isEmpty()) return;
        if (hit.drone_a == null || hit.drone_b == null) return;

        touchIdle(ctx);

        // 1) pair 归一化（a <= b），并同步坐标（保证 a 坐标永远对应 a）
        String a = hit.drone_a;
        String b = hit.drone_b;

        double ax = hit.ax, ay = hit.ay, az = hit.az;
        double bx = hit.bx, by = hit.by, bz = hit.bz;

        if (a.compareTo(b) > 0) {
            String tmp = a; a = b; b = tmp;
            double tx = ax; ax = bx; bx = tx;
            double ty = ay; ay = by; by = ty;
            double tz = az; az = bz; bz = tz;
        }

        final String pairKey = a + "|" + b;
        final long f = hit.frame_t_ms;

        CollisionSegState seg = openSegByPair.get(pairKey);

        if (seg == null) {
            // 开新段
            seg = CollisionSegState.startFromHit(hit, a, b, ax, ay, az, bx, by, bz, frameMs);
            openSegByPair.put(pairKey, seg);
            return;
        }

        // 保护：重复 hit/乱序（at-least-once 可能重复）
        if (seg.last_frame_t_ms != null && f <= seg.last_frame_t_ms) {
            return;
        }

        // gap 切段：输出旧段 -> 开新段
        if (seg.last_frame_t_ms != null && (f - seg.last_frame_t_ms) > maxGapMs) {
            out.collect(seg.toEvent());
            CollisionSegState newSeg = CollisionSegState.startFromHit(hit, a, b, ax, ay, az, bx, by, bz, frameMs);
            openSegByPair.put(pairKey, newSeg);
            return;
        }

        // 连续/小间隙：更新当前段
        seg.updateWithHit(hit, ax, ay, az, bx, by, bz, frameMs);
        openSegByPair.put(pairKey, seg);

        // 如果你想“段内实时可见”，可以在这里 out.collect(seg.toEvent()) 做覆盖更新（先别开）
    }

    // ============= 输入2：run_end_marker（从 traj topic 分流）=============
    @Override
    public void processElement2(OdsTrajPoint m, Context ctx, Collector<DwsCollisionEvent> out) throws Exception {
        if (isFinalized()) return;
        if (m == null || m.run_id == null || m.run_id.isEmpty()) return;
        if (m.event_type == null || !"run_end_marker".equals(m.event_type)) return;

        touchIdle(ctx);

        // producer 口径：marker.t_ms = end_ms + frameMs
        long endMs = m.t_ms - frameMs;

        Long exist = finalizeTimerTs.value();
        if (exist == null || endMs > exist) {
            if (exist != null) ctx.timerService().deleteEventTimeTimer(exist);
            ctx.timerService().registerEventTimeTimer(endMs);
            finalizeTimerTs.update(endMs);
        }
    }

    // ============= timer：event-time 定稿 flush + processing-time 兜底 flush =============
    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<DwsCollisionEvent> out) throws Exception {
        if (isFinalized()) return;

        if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
            Long fts = finalizeTimerTs.value();
            if (fts == null || fts != ts) return;

            flushAllOpenSegments(out);
            markFinalized();
            clearAll();
            return;
        }

        // processing-time idle flush（兜底）
        Long its = idleTimerTs.value();
        if (its == null || its != ts) return;

        Long last = lastSeenProcTs.value();
        if (last == null) return;

        if (ts - last >= IDLE_FLUSH_MS) {
            flushAllOpenSegments(out);
            markFinalized();
            clearAll();
        } else {
            ensureIdleTimer(ctx, ts);
        }
    }

    // ---------------- helpers ----------------
    private void flushAllOpenSegments(Collector<DwsCollisionEvent> out) throws Exception {
        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, CollisionSegState> e : openSegByPair.entries()) {
            keys.add(e.getKey());
        }
        for (String k : keys) {
            CollisionSegState seg = openSegByPair.get(k);
            if (seg != null) out.collect(seg.toEvent());
        }
        openSegByPair.clear();
    }

    private void touchIdle(Context ctx) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        lastSeenProcTs.update(now);
        ensureIdleTimer(ctx, now);
    }

    private void ensureIdleTimer(Context ctx, long now) throws Exception {
        Long exist = idleTimerTs.value();
        long next = now + IDLE_FLUSH_MS;
        if (exist == null || exist != next) {
            if (exist != null) ctx.timerService().deleteProcessingTimeTimer(exist);
            ctx.timerService().registerProcessingTimeTimer(next);
            idleTimerTs.update(next);
        }
    }

    private void ensureIdleTimer(OnTimerContext ctx, long now) throws Exception {
        Long exist = idleTimerTs.value();
        long next = now + IDLE_FLUSH_MS;
        if (exist == null || exist != next) {
            if (exist != null) ctx.timerService().deleteProcessingTimeTimer(exist);
            ctx.timerService().registerProcessingTimeTimer(next);
            idleTimerTs.update(next);
        }
    }

    private boolean isFinalized() throws Exception {
        Boolean b = finalized.value();
        return b != null && b;
    }

    private void markFinalized() throws Exception {
        finalized.update(true);
    }

    private void clearAll() throws Exception {
        openSegByPair.clear();
        finalizeTimerTs.clear();
        lastSeenProcTs.clear();
        idleTimerTs.clear();
        // finalized 不清：防重复 flush
    }
}

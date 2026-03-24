package com.rtw.ops;

import com.rtw.model.DwdTrajPoint;
import com.rtw.model.DwsOverspeedEvent;
import com.rtw.util.TimeUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Overspeed segment:
 * - 连续 is_overspeed=1 合并成一条事件
 * - 碰到 is_overspeed=0 则关闭段并输出
 * - 增加 idle flush：断流/结束时段没关闭也能输出（工程兜底）
 *
 * key 建议：run_id + "|" + drone_id
 */
public class OverspeedSegmentFn extends KeyedProcessFunction<String, DwdTrajPoint, DwsOverspeedEvent> {

    private static final long IDLE_FLUSH_MS = 15000;

    // ===== 新增：防抖与鲁棒性参数（你也可以后续放 AppConfig）=====
    private static final int MIN_POINTS = 3;          // 少于3点的段可认为噪声
    private static final int CLOSE_AFTER_NON_OS = 2;  // 连续2个非超速才结束
    private static final int MAX_SEQ_GAP = 20;        // seq 跳太大则强制切段

    private transient ValueState<Seg> segState;
    private transient ValueState<Integer> nonOsStreak;     // 新增：非超速连续计数
    private transient ValueState<Integer> lastSeq;         // 新增：去重/乱序/跳变保护

    private transient ValueState<Long> lastSeenProcTs;
    private transient ValueState<Long> idleTimerTs;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        segState = getRuntimeContext().getState(new ValueStateDescriptor<>("osSeg", Seg.class));
        nonOsStreak = getRuntimeContext().getState(new ValueStateDescriptor<>("osNonOsStreak", Integer.class));
        lastSeq = getRuntimeContext().getState(new ValueStateDescriptor<>("osLastSeq", Integer.class));

        lastSeenProcTs = getRuntimeContext().getState(new ValueStateDescriptor<>("osLastSeen", Long.class));
        idleTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("osIdleTimer", Long.class));
    }

    @Override
    public void processElement(DwdTrajPoint p, Context ctx, Collector<DwsOverspeedEvent> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        lastSeenProcTs.update(now);
        ensureIdleTimer(ctx, now);

        // ===== 1) 去重/乱序保护 =====
        Integer ls = lastSeq.value();
        if (ls != null && p.seq <= ls) {
            return; // 重复或乱序点：直接忽略（保证统计不被污染）
        }
        // seq gap 保护：gap 太大 -> flush 当前段（如果存在）
        if (ls != null && (p.seq - ls) > MAX_SEQ_GAP) {
            Seg seg = segState.value();
            if (seg != null && seg.cnt >= MIN_POINTS) out.collect(seg.toEvent());
            segState.clear();
            nonOsStreak.clear();
        }
        lastSeq.update(p.seq);

        boolean os = p.is_overspeed != null && p.is_overspeed == 1;
        Seg seg = segState.value();
        Integer streak = nonOsStreak.value();
        if (streak == null) streak = 0;

        if (os) {
            // ===== 2) 超速：更新段 & 清零非超速 streak =====
            if (seg == null) seg = Seg.start(p);
            seg.update(p);
            segState.update(seg);
            nonOsStreak.update(0);
        } else {
            // ===== 3) 非超速：不立刻 close，而是累计 streak =====
            if (seg != null) {
                streak += 1;
                if (streak >= CLOSE_AFTER_NON_OS) {
                    if (seg.cnt >= MIN_POINTS) out.collect(seg.toEvent());
                    segState.clear();
                    nonOsStreak.clear();
                } else {
                    nonOsStreak.update(streak);
                }
            }
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<DwsOverspeedEvent> out) throws Exception {
        Long myTs = idleTimerTs.value();
        if (myTs == null || myTs != ts) return;

        Long last = lastSeenProcTs.value();
        if (last == null) return;

        if (ts - last >= IDLE_FLUSH_MS) {
            Seg seg = segState.value();
            if (seg != null && seg.cnt >= MIN_POINTS) out.collect(seg.toEvent());
            segState.clear();
            nonOsStreak.clear();
            idleTimerTs.clear();
            lastSeq.clear();
        } else {
            ensureIdleTimer(ctx, last);
        }
    }

    private void ensureIdleTimer(Context ctx, long now) throws Exception {
        if (idleTimerTs.value() != null) return;
        long fire = now + IDLE_FLUSH_MS;
        ctx.timerService().registerProcessingTimeTimer(fire);
        idleTimerTs.update(fire);
    }


    // ===== 段状态（只放标量，便于 state 序列化稳定）=====
    public static class Seg {
        public String dt;
        public String run_id;
        public String drone_id;

        public int start_seq;
        public long start_t_ms;
        public double start_x, start_y, start_z;

        public int end_seq;
        public long end_t_ms;
        public double end_x, end_y, end_z;

        public int cnt;
        public double sumSpeed;
        public double maxSpeed;

        public String strategy_id;
        public String strategy_version;
        public String param_set_id;
        public Integer seed;

        public Seg() {}

        public static Seg start(DwdTrajPoint p) {
            Seg s = new Seg();
            s.dt = (p.dt != null) ? p.dt : TimeUtil.dtUtc(p.t_ms);
            s.run_id = p.run_id;
            s.drone_id = p.drone_id;

            s.start_seq = p.seq;
            s.start_t_ms = p.t_ms;
            s.start_x = p.x; s.start_y = p.y; s.start_z = p.z;

            s.end_seq = p.seq;
            s.end_t_ms = p.t_ms;
            s.end_x = p.x; s.end_y = p.y; s.end_z = p.z;

            s.strategy_id = p.strategy_id;
            s.strategy_version = p.strategy_version;
            s.param_set_id = p.param_set_id;
            s.seed = p.seed;

            s.cnt = 0;
            s.sumSpeed = 0.0;
            s.maxSpeed = Double.NEGATIVE_INFINITY;
            return s;
        }

        public void update(DwdTrajPoint p) {
            this.end_seq = p.seq;
            this.end_t_ms = p.t_ms;
            this.end_x = p.x; this.end_y = p.y; this.end_z = p.z;

            double sp = (p.speed == null) ? 0.0 : p.speed;
            this.cnt += 1;
            this.sumSpeed += sp;
            this.maxSpeed = Math.max(this.maxSpeed, sp);
        }

        public DwsOverspeedEvent toEvent() {
            DwsOverspeedEvent e = new DwsOverspeedEvent();
            e.dt = this.dt;
            e.run_id = this.run_id;
            e.drone_id = this.drone_id;

            // 方案A：UNIQUE KEY(dt,run_id,event_id)，event_id run 内唯一即可
            e.event_id = this.drone_id + "|overspeed|" + this.start_seq;

            e.start_seq = this.start_seq;
            e.end_seq = this.end_seq;
            e.start_t_ms = this.start_t_ms;
            e.end_t_ms = this.end_t_ms;
            e.duration_ms = Math.max(0L, this.end_t_ms - this.start_t_ms);

            e.points_cnt = this.cnt;
            e.max_speed = (this.maxSpeed == Double.NEGATIVE_INFINITY) ? null : this.maxSpeed;
            e.avg_speed = (this.cnt > 0) ? (this.sumSpeed / this.cnt) : null;

            e.start_x = this.start_x; e.start_y = this.start_y; e.start_z = this.start_z;
            e.end_x = this.end_x; e.end_y = this.end_y; e.end_z = this.end_z;

            e.strategy_id = this.strategy_id;
            e.strategy_version = this.strategy_version;
            e.param_set_id = this.param_set_id;
            e.seed = this.seed;

            e.update_ms = System.currentTimeMillis();
            return e;
        }
    }
}

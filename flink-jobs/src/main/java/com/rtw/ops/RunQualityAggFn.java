package com.rtw.ops;

import com.rtw.config.AppConfig;
import com.rtw.model.DwdTrajPoint;
import com.rtw.model.DwsRunQuality;
import com.rtw.model.OdsTrajPoint;
import com.rtw.util.TimeUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * run 级质量汇总：
 * - 输入1：DWD 点流（已经带上 run_meta 的维度信息 / 动态阈值规则结果）
 * - 输入2：run_end_marker（来自同一个 traj topic 的分流），用于“事件时间”定稿
 *
 * 关键：定稿不再用 processing-time grace，而是用 event-time timer(end_ms)。
 */
public class RunQualityAggFn extends KeyedCoProcessFunction<String, DwdTrajPoint, OdsTrajPoint, DwsRunQuality> {

    private final AppConfig cfg;

    // ===== 输出模式 =====
    public enum EmitMode { FINAL_ONLY, INCR_AND_FINAL }
    private final EmitMode mode;
    private final long emitIntervalMs;
    private final int topN;

    // ===== score 权重 =====
    private final int wOs;
    private final int wOa;
    private final int wTp;

    // ===== state =====
    private transient ValueState<Long> overspeedCnt;
    private transient ValueState<Long> overaccCnt;
    private transient ValueState<Long> teleportCnt;

    private transient MapState<String, DroneAgg> droneAgg;
    private transient ValueState<MetaSnap> metaSnap;

    private transient ValueState<Long> periodicTimerTs;  // processing-time
    private transient ValueState<Long> finalizeTimerTs;  // event-time

    private transient ValueState<Boolean> endSeen;
    private transient ValueState<Boolean> finalized;

    private transient ObjectMapper mapper;

    public RunQualityAggFn(AppConfig cfg) {
        this.cfg = cfg;
        this.mode = EmitMode.valueOf(cfg.dwsEmitMode);
        this.emitIntervalMs = cfg.dwsEmitIntervalMs;
        this.topN = cfg.dwsTopN;
        this.wOs = cfg.scoreWOs;
        this.wOa = cfg.scoreWOa;
        this.wTp = cfg.scoreWTp;
    }

    @Override
    public void open(Configuration parameters) {
        overspeedCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("osCnt", Long.class));
        overaccCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("oaCnt", Long.class));
        teleportCnt = getRuntimeContext().getState(new ValueStateDescriptor<>("tpCnt", Long.class));

        droneAgg = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("droneAgg", Types.STRING, Types.POJO(DroneAgg.class))
        );

        metaSnap = getRuntimeContext().getState(new ValueStateDescriptor<>("metaSnap", MetaSnap.class));

        periodicTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("periodicTimerTs", Long.class));
        finalizeTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("finalizeTimerTs", Long.class));

        endSeen = getRuntimeContext().getState(new ValueStateDescriptor<>("endSeen", Boolean.class));
        finalized = getRuntimeContext().getState(new ValueStateDescriptor<>("finalized", Boolean.class));

        mapper = new ObjectMapper();
    }

    // 输入1：点流
    @Override
    public void processElement1(DwdTrajPoint p, Context ctx, Collector<DwsRunQuality> out) throws Exception {
        if (isFinalized()) return;

        // ===== meta 补齐（只填一次，保证 UNIQUE KEY 稳定） =====
        MetaSnap ms = getOrCreateMeta(ctx.getCurrentKey());
        if (ms.dt == null && p.dt != null) ms.dt = p.dt;
        // start_ms：用点流最早的 t_ms 近似（run_meta 不接入本算子也能算 duration）
        if (ms.start_ms == null || p.t_ms < ms.start_ms) ms.start_ms = p.t_ms;

        if (ms.scenario_id == null && p.scenario_id != null) ms.scenario_id = p.scenario_id;
        if (ms.strategy_id == null && p.strategy_id != null) ms.strategy_id = p.strategy_id;
        if (ms.strategy_version == null && p.strategy_version != null) ms.strategy_version = p.strategy_version;
        if (ms.param_set_id == null && p.param_set_id != null) ms.param_set_id = p.param_set_id;
        if (ms.seed == null && p.seed != null) ms.seed = p.seed;
        metaSnap.update(ms);

        int os = one(p.is_overspeed);
        int oa = one(p.is_overacc);
        int tp = one(p.is_teleport);

        if (os == 1) overspeedCnt.update(nvl(overspeedCnt.value()) + 1);
        if (oa == 1) overaccCnt.update(nvl(overaccCnt.value()) + 1);
        if (tp == 1) teleportCnt.update(nvl(teleportCnt.value()) + 1);

        DroneAgg a = droneAgg.get(p.drone_id);
        if (a == null) a = new DroneAgg();
        a.drone_id = p.drone_id;
        a.overspeed += os;
        a.overacc += oa;
        a.teleport += tp;
        droneAgg.put(p.drone_id, a);

        // INCR_AND_FINAL：启动周期快照 timer（若 end 未到）
        if (mode == EmitMode.INCR_AND_FINAL && !isEndSeen()) {
            ensurePeriodicTimer(ctx);
        }
    }

    // 输入2：run_end_marker（同 topic 分流出来）
    @Override
    public void processElement2(OdsTrajPoint m, Context ctx, Collector<DwsRunQuality> out) throws Exception {
        if (isFinalized()) return;
        if (m == null || m.run_id == null || m.run_id.isEmpty()) return;
        if (m.event_type == null || !"run_end_marker".equals(m.event_type)) return;

        MetaSnap ms = getOrCreateMeta(ctx.getCurrentKey());

        ms.run_id = m.run_id;

        endSeen.update(true);

        // end_ms：marker.t_ms = end_ms + trajFrameMs（默认 40ms）
        long endMs = m.t_ms - cfg.trajFrameMs;
        ms.end_ms = endMs;
        if (ms.dt == null) ms.dt = TimeUtil.dtUtc(endMs);

        // status：优先用 marker.status
        if (m.status != null && !m.status.isEmpty()) {
            ms.status = m.status;
        } else if (ms.status == null) {
            ms.status = "success";
        }

        // duration：若 start_ms 已有（点流会补），可算出来
        if (ms.start_ms != null) ms.duration_ms = ms.end_ms - ms.start_ms;

        // drone_cnt：marker 给了就记录下来（否则 buildResult 用 state map size）
        if (m.drone_count != null) ms.drone_cnt = m.drone_count;

        // 关键：event-time 定稿 timer（按 end_ms 注册）
        Long exist = finalizeTimerTs.value();
        if (exist == null) {
            ctx.timerService().registerEventTimeTimer(endMs);
            finalizeTimerTs.update(endMs);
        }

        // end 到了就不再需要周期快照（不取消 timer，但我们会忽略）
        periodicTimerTs.clear();

        metaSnap.update(ms);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DwsRunQuality> out) throws Exception {
        if (isFinalized()) return;

        if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
            Long fin = finalizeTimerTs.value();
            if (fin != null && fin == timestamp) {
                // event-time 定稿
                DwsRunQuality r = buildResult(ctx.getCurrentKey(), 1, System.currentTimeMillis());
                if (r != null) out.collect(r);

                // 清理并标记 finalized
                clearAllState();
                finalized.update(true);
            }
            return;
        }

        // processing-time：周期快照
        if (mode == EmitMode.INCR_AND_FINAL && !isEndSeen()) {
            Long pt = periodicTimerTs.value();
            if (pt != null && pt == timestamp) {
                DwsRunQuality r = buildResult(ctx.getCurrentKey(), 0, System.currentTimeMillis());
                if (r != null) out.collect(r);

                long next = timestamp + emitIntervalMs;
                ctx.timerService().registerProcessingTimeTimer(next);
                periodicTimerTs.update(next);
            }
        }
    }

    private void ensurePeriodicTimer(Context ctx) throws Exception {
        if (periodicTimerTs.value() != null) return;
        long t = ctx.timerService().currentProcessingTime() + emitIntervalMs;
        ctx.timerService().registerProcessingTimeTimer(t);
        periodicTimerTs.update(t);
    }

    private DwsRunQuality buildResult(String runId, int isFinal, long snapshotMs) throws Exception {
        MetaSnap ms = metaSnap.value();
        if (ms == null || ms.dt == null) return null; // 没有 dt 不能落 Doris UNIQUE KEY

        DwsRunQuality r = new DwsRunQuality();
        r.dt = ms.dt;
        r.run_id = runId;

        r.scenario_id = ms.scenario_id;
        r.strategy_id = ms.strategy_id;
        r.strategy_version = ms.strategy_version;
        r.param_set_id = ms.param_set_id;
        r.seed = ms.seed;

        r.start_ms = ms.start_ms;
        r.end_ms = ms.end_ms;
        r.duration_ms = ms.duration_ms;
        r.status = ms.status != null ? ms.status : (isFinal == 1 ? "success" : "running");

        r.overspeed_cnt = nvl(overspeedCnt.value());
        r.overacc_cnt = nvl(overaccCnt.value());
        r.teleport_cnt = nvl(teleportCnt.value());

        // drone_cnt：优先用 marker 给的，否则用 state 中见过的 drone 数
        int seenDroneCnt = 0;
        List<DroneAgg> list = new ArrayList<>();
        for (Map.Entry<String, DroneAgg> e : droneAgg.entries()) {
            list.add(e.getValue());
            seenDroneCnt++;
        }
        r.drone_cnt = (ms.drone_cnt != null) ? ms.drone_cnt : seenDroneCnt;

        // TopN：按 score 排序
        list.sort(Comparator.comparingInt(a -> -score(a)));
        int limit = Math.min(topN, list.size());

        List<Map<String, Object>> top = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            DroneAgg a = list.get(i);
            Map<String, Object> m = new HashMap<>();
            m.put("drone_id", a.drone_id);
            m.put("overspeed", a.overspeed);
            m.put("overacc", a.overacc);
            m.put("teleport", a.teleport);
            m.put("score", score(a));
            top.add(m);
        }
        r.top_drones_json = mapper.writeValueAsString(top);

        r.update_ms = snapshotMs;
        r.is_final = isFinal;
        r.snapshot_ms = snapshotMs;

        Map<String, Integer> w = new HashMap<>();
        w.put("w_os", wOs);
        w.put("w_oa", wOa);
        w.put("w_tp", wTp);
        r.score_weights_json = mapper.writeValueAsString(w);

        return r;
    }

    private int score(DroneAgg a) {
        return a.overspeed * wOs + a.overacc * wOa + a.teleport * wTp;
    }

    private MetaSnap getOrCreateMeta(String runId) throws Exception {
        MetaSnap ms = metaSnap.value();
        if (ms == null) ms = new MetaSnap();
        if (ms.run_id == null) ms.run_id = runId;
        return ms;
    }


    private boolean isEndSeen() throws Exception {
        Boolean b = endSeen.value();
        return b != null && b;
    }

    private boolean isFinalized() throws Exception {
        Boolean b = finalized.value();
        return b != null && b;
    }

    private void clearAllState() throws Exception {
        overspeedCnt.clear();
        overaccCnt.clear();
        teleportCnt.clear();
        droneAgg.clear();
        metaSnap.clear();
        periodicTimerTs.clear();
        finalizeTimerTs.clear();
        endSeen.clear();
    }

    private static long nvl(Long v) { return v == null ? 0L : v; }
    private static int one(Integer v) { return (v != null && v == 1) ? 1 : 0; }

    // ===== state POJO =====
    public static class DroneAgg {
        public String drone_id;
        public int overspeed;
        public int overacc;
        public int teleport;
    }

    public static class MetaSnap {
        public String dt;
        public String run_id;

        public String scenario_id;
        public String strategy_id;
        public String strategy_version;
        public String param_set_id;
        public Integer seed;

        public Long start_ms;
        public Long end_ms;
        public Long duration_ms;
        public String status;

        public Integer drone_cnt; // marker 里给的 drone_count（可选）
    }
}

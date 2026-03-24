package com.rtw.ops;

import com.rtw.config.AppConfig;
import com.rtw.model.DwdTrajPoint;
import com.rtw.model.OdsRunMeta;
import com.rtw.model.OdsTrajPoint;
import com.rtw.util.TimeUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class DwdDiffWithMetaFn extends KeyedBroadcastProcessFunction<String, OdsTrajPoint, OdsRunMeta, DwdTrajPoint> {

    /** 广播状态：run_id -> RunParams */
    public static final MapStateDescriptor<String, RunParams> RUN_PARAMS_DESC =
            new MapStateDescriptor<>("run_params", Types.STRING, Types.POJO(RunParams.class));

    private final AppConfig cfg;
    private transient ValueState<LastPoint> lastState;
    private transient ObjectMapper mapper;

    public DwdDiffWithMetaFn(AppConfig cfg) { this.cfg = cfg; }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
               // lastState：run_id|drone_id 级“上一点”。必须加 TTL，避免异常结束/长尾 key 造成状态膨胀。
                       ValueStateDescriptor<LastPoint> desc = new ValueStateDescriptor<>("lastPoint", LastPoint.class);
                StateTtlConfig ttl = StateTtlConfig
                               // TTL：按你的 run 最长时长调；MVP 先给 2 小时兜底。
                               .newBuilder(Time.hours(2))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                desc.enableTimeToLive(ttl);
                lastState = getRuntimeContext().getState(desc);
        mapper = new ObjectMapper();
    }

    // ========== 处理 traj 点（读广播阈值 -> enrich -> 计算 DWD） ==========
    @Override
    public void processElement(OdsTrajPoint cur, ReadOnlyContext ctx, Collector<DwdTrajPoint> out) throws Exception {
        // 1) 取 run 的动态阈值（没有就 fallback）
        ReadOnlyBroadcastState<String, RunParams> st = ctx.getBroadcastState(RUN_PARAMS_DESC);
        RunParams p = st.get(cur.run_id);

        double teleportDist = (p != null) ? p.teleportDistM : cfg.teleportDistM;
        double vMax = (p != null) ? p.vMax : cfg.vMax;
        double aMax = (p != null) ? p.aMax : cfg.aMax;

        // 2) 复用你原 DwdDiffFn 的差分逻辑（每个 run|drone 维护 lastPoint）
        LastPoint last = lastState.value();

        DwdTrajPoint d = new DwdTrajPoint();
        d.dt = TimeUtil.dtUtc(cur.t_ms);   // ✅ dt 固化：UTC from t_ms

        d.run_id = cur.run_id;
        d.drone_id = cur.drone_id;
        d.seq = cur.seq;

        d.scenario_id = cur.scenario_id;
        d.t_ms = cur.t_ms;
        d.x = cur.x; d.y = cur.y; d.z = cur.z;

        d.strategy_id = cur.strategy_id;
        d.strategy_version = cur.strategy_version;
        d.param_set_id = cur.param_set_id;
        d.seed = cur.seed;

        if (last == null) {
            d.is_time_back = 0;
            d.is_teleport = 0;
            d.is_overspeed = 0;
            d.is_overacc = 0;
            d.is_stuck = 0;
            lastState.update(new LastPoint(cur.t_ms, cur.x, cur.y, cur.z, null));
            out.collect(d);
            return;
        }

        long dtMs = cur.t_ms - last.tMs;
        d.dt_ms = dtMs;

        // 乱序保护：dt<=0 不做差分，避免 speed/acc 爆炸
        if (dtMs <= 0) {
            d.is_time_back = 1;
            d.is_teleport = 0;
            d.is_overspeed = 0;
            d.is_overacc = 0;
            d.is_stuck = 0;

            if (cur.t_ms > last.tMs) lastState.update(new LastPoint(cur.t_ms, cur.x, cur.y, cur.z, null));
            out.collect(d);
            return;
        }

        double dx = cur.x - last.x;
        double dy = cur.y - last.y;
        double dz = cur.z - last.z;
        d.dx = dx; d.dy = dy; d.dz = dz;

        double dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
        d.is_teleport = (dist > teleportDist) ? 1 : 0;

        double dtSec = dtMs / 1000.0;
        double speed = dist / dtSec;
        d.speed = speed;

        if (last.speed == null) d.acc = null;
        else d.acc = (speed - last.speed) / dtSec;

        d.is_time_back = 0;
        d.is_overspeed = (d.speed != null && d.speed > vMax) ? 1 : 0;
        d.is_overacc = (d.acc != null && Math.abs(d.acc) > aMax) ? 1 : 0;
        d.is_stuck = 0;

        lastState.update(new LastPoint(cur.t_ms, cur.x, cur.y, cur.z, speed));
        out.collect(d);
    }

    // ========== 处理 run_meta（解析 param_json -> 写广播状态） ==========
    @Override
    public void processBroadcastElement(OdsRunMeta meta, Context ctx, Collector<DwdTrajPoint> out) throws Exception {
        if (meta == null || meta.run_id == null || meta.run_id.isEmpty()) return;

        BroadcastState<String, RunParams> st = ctx.getBroadcastState(RUN_PARAMS_DESC);

        // MVP：如果收到了 end/status，清理 state，避免无限增长（默认认为 end 后不会再来点）
        if (meta.status != null && !meta.status.isEmpty()) {
            st.remove(meta.run_id);
            return;
        }

        // 解析 param_json，覆盖 fallback
        RunParams p = parseParams(meta.param_json, meta.strategy_id);
        st.put(meta.run_id, p);
    }

    private RunParams parseParams(String paramJson, String strategyId) {
        double vMax = cfg.vMax;
        double aMax = cfg.aMax;
        double teleport = cfg.teleportDistM;

        if (paramJson != null && !paramJson.isEmpty()) {
            try {
                JsonNode root = mapper.readTree(paramJson);

                // strategy_id 优先用 run_meta 顶层，其次从 transition.method 推导
                String sid = strategyId;
                if (sid == null || sid.isEmpty()) {
                    JsonNode transition = root.path("transition");
                    if (transition.isObject()) {
                        String m = transition.path("method").asText("");
                        if (!m.isEmpty()) sid = m;
                    }
                }
                if (sid == null || sid.isEmpty()) sid = "orca";

                // 1) 先取算法内覆盖：<strategy>.limits.v_xy_max / a_xy_max
                JsonNode strategyLimits = root.path(sid).path("limits");
                if (strategyLimits.isObject()) {
                    if (strategyLimits.has("v_xy_max")) {
                        vMax = strategyLimits.get("v_xy_max").asDouble(vMax);
                    }
                    if (strategyLimits.has("a_xy_max")) {
                        aMax = strategyLimits.get("a_xy_max").asDouble(aMax);
                    }
                }

                // 2) 再取全局 limits 兜底
                JsonNode globalLimits = root.path("limits");
                if (globalLimits.isObject()) {
                    if (globalLimits.has("v_xy_max") && vMax == cfg.vMax) {
                        vMax = globalLimits.get("v_xy_max").asDouble(vMax);
                    }
                    if (globalLimits.has("a_xy_max") && aMax == cfg.aMax) {
                        aMax = globalLimits.get("a_xy_max").asDouble(aMax);
                    }
                }

                // 3) teleport_dist_m：优先显式字段，否则继续走 cfg 默认
                if (root.has("teleport_dist_m")) {
                    teleport = root.get("teleport_dist_m").asDouble(teleport);
                } else if (root.path("param").has("teleport_dist_m")) {
                    teleport = root.path("param").get("teleport_dist_m").asDouble(teleport);
                }
            } catch (Exception ignored) {
                // 解析失败就用 fallback
            }
        }
        return new RunParams(vMax, aMax, teleport, System.currentTimeMillis());
    }



    /** 广播态 value：必须 POJO（public 字段 + 无参构造） */
    public static class RunParams {
        public double vMax;
        public double aMax;
        public double teleportDistM;
        public long updatedAtMs;

        public RunParams() {}
        public RunParams(double vMax, double aMax, double teleportDistM, long updatedAtMs) {
            this.vMax = vMax;
            this.aMax = aMax;
            this.teleportDistM = teleportDistM;
            this.updatedAtMs = updatedAtMs;
        }
    }

    /** Keyed state：上一点 */
    public static class LastPoint {
        public long tMs;
        public double x, y, z;
        public Double speed;
        public LastPoint() {}
        public LastPoint(long tMs, double x, double y, double z, Double speed) {
            this.tMs = tMs; this.x = x; this.y = y; this.z = z; this.speed = speed;
        }
    }
}

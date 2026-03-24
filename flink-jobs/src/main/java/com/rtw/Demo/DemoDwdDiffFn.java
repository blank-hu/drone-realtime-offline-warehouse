package com.rtw.Demo;

import com.rtw.config.AppConfig;
import com.rtw.model.DwdTrajPoint;
import com.rtw.model.OdsTrajPoint;
import com.rtw.util.TimeUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DemoDwdDiffFn extends KeyedProcessFunction<String, OdsTrajPoint, DwdTrajPoint> {

    private final AppConfig cfg;
    private transient ValueState<LastPoint> lastState;

    public DemoDwdDiffFn(AppConfig cfg) { this.cfg = cfg; }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        lastState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPoint", LastPoint.class));
    }

    @Override
    public void processElement(OdsTrajPoint cur, Context ctx, Collector<DwdTrajPoint> out) throws Exception {
        LastPoint last = lastState.value();

        DwdTrajPoint d = new DwdTrajPoint();
        d.dt = TimeUtil.dtUtc(cur.t_ms);       // ✅ Step1 固化：dt from t_ms by UTC
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
            // 第一条：没有上一点，差分为空
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

            // 只在时间更大时更新 last
            if (cur.t_ms > last.tMs) lastState.update(new LastPoint(cur.t_ms, cur.x, cur.y, cur.z, null));
            out.collect(d);
            return;
        }

        double dx = cur.x - last.x;
        double dy = cur.y - last.y;
        double dz = cur.z - last.z;
        d.dx = dx; d.dy = dy; d.dz = dz;

        double dist = Math.sqrt(dx*dx + dy*dy + dz*dz);
        d.is_teleport = (dist > cfg.teleportDistM) ? 1 : 0;

        double dtSec = dtMs / 1000.0;
        double speed = dist / dtSec;
        d.speed = speed;

        if (last.speed == null) d.acc = null;
        else d.acc = (speed - last.speed) / dtSec;

        d.is_time_back = 0;
        d.is_overspeed = (d.speed != null && d.speed > cfg.vMax) ? 1 : 0;
        d.is_overacc = (d.acc != null && Math.abs(d.acc) > cfg.aMax) ? 1 : 0;
        d.is_stuck = 0; // 后续专门做“持续时间”判定

        lastState.update(new LastPoint(cur.t_ms, cur.x, cur.y, cur.z, speed));
        out.collect(d);
    }

    /** Flink State 需要无参构造 */
    public static class LastPoint {
        public long tMs;
        public double x, y, z;
        public Double speed;
        public LastPoint() {}
        public LastPoint(long tMs, double x, double y, double z, Double speed) {
            this.tMs = tMs; this.x=x; this.y=y; this.z=z; this.speed=speed;
        }
    }
}

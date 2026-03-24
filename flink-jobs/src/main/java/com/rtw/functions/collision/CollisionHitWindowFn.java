package com.rtw.functions.collision;


import com.rtw.model.CollisionHit;
import com.rtw.model.FramePoint;
import com.rtw.model.DwdTrajPoint;  // 你项目里的 DWD 点 POJO（按你实际包名改）

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class CollisionHitWindowFn extends ProcessWindowFunction<DwdTrajPoint, CollisionHit, String, TimeWindow> {

    private final double dMin;
    private final double cell;
    private final List<int[]> neigh = SpatialGrid.neighbors27();

    public CollisionHitWindowFn(double dMin) {
        this.dMin = dMin;
        this.cell = dMin; // cell size = d_min
    }

    @Override
    public void process(String runId,
                        Context ctx,
                        Iterable<DwdTrajPoint> elements,
                        Collector<CollisionHit> out) {

        final long frameStart = ctx.window().getStart(); // 强烈建议用 window start 作为 frame_t_ms
        final long now = System.currentTimeMillis();

        // 1) 去重：每 drone 只保留 t_ms 最大的一条
        Map<String, FramePoint> latest = new HashMap<>();
        for (DwdTrajPoint p : elements) {
            if (p == null || p.drone_id == null) continue;
            FramePoint fp = latest.get(p.drone_id);
            if (fp == null || ( p.t_ms > fp.tMs)) {
                FramePoint n = new FramePoint();
                n.runId = p.run_id;
                n.droneId = p.drone_id;
                n.tMs = p.t_ms;
                n.x = p.x; n.y = p.y; n.z = p.z;
                n.scenarioId = p.scenario_id;
                n.strategyId = p.strategy_id;
                n.strategyVersion = p.strategy_version;
                n.paramSetId = p.param_set_id;
                n.seed = p.seed;
                latest.put(p.drone_id, n);
            }
        }
        if (latest.size() < 2) return;

        // 2) 建网格
        Map<Long, List<FramePoint>> grid = new HashMap<>();
        for (FramePoint fp : latest.values()) {
            int ix = (int) Math.floor(fp.x / cell);
            int iy = (int) Math.floor(fp.y / cell);
            int iz = (int) Math.floor(fp.z / cell);
            long key = SpatialGrid.cellKey(ix, iy, iz);
            grid.computeIfAbsent(key, k -> new ArrayList<>()).add(fp);
        }

        // 3) 邻域比较输出 hit
        for (FramePoint a : latest.values()) {
            int aix = (int) Math.floor(a.x / cell);
            int aiy = (int) Math.floor(a.y / cell);
            int aiz = (int) Math.floor(a.z / cell);

            for (int[] d : neigh) {
                long nk = SpatialGrid.cellKey(aix + d[0], aiy + d[1], aiz + d[2]);
                List<FramePoint> cand = grid.get(nk);
                if (cand == null) continue;

                for (FramePoint b : cand) {
                    if (a.droneId.equals(b.droneId)) continue;

                    // 关键：固定排序，避免重复输出
                    String da = a.droneId;
                    String db = b.droneId;
                    if (da.compareTo(db) >= 0) continue; // 只输出 da < db

                    double dx = a.x - b.x, dy = a.y - b.y, dz = a.z - b.z;
                    double dist = Math.sqrt(dx*dx + dy*dy + dz*dz);
                    if (dist > dMin) continue;

                    CollisionHit hit = new CollisionHit();
                    hit.run_id = runId;
                    hit.frame_t_ms = frameStart;
                    hit.drone_a = da;
                    hit.drone_b = db;
                    hit.dist = dist;
                    hit.d_min = dMin;

                    hit.ax = a.x; hit.ay = a.y; hit.az = a.z;
                    hit.bx = b.x; hit.by = b.y; hit.bz = b.z;

                    // 追溯维度：取 a 的即可（同 run 口径一致）
                    hit.scenario_id = a.scenarioId;
                    hit.strategy_id = a.strategyId;
                    hit.strategy_version = a.strategyVersion;
                    hit.param_set_id = a.paramSetId;
                    hit.seed = a.seed;

                    hit.update_ms = now;
                    out.collect(hit);
                }
            }
        }
    }
}

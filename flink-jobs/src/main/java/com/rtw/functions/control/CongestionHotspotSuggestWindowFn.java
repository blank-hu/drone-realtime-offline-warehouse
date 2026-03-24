package com.rtw.functions.control;

import com.rtw.config.AppConfig;
import com.rtw.functions.collision.SpatialGrid;
import com.rtw.model.DwdTrajPoint;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 两段式：cell 预聚合 -> 热点精检 -> control_suggestion(JSON String)
 *
 * 设计目标：
 *  - 不引入跨窗口大状态：所有 Map/List 都是窗口内局部变量
 *  - 只在“热点 cell”做邻域精检，避免全量 O(N^2)
 *  - 输出建议先发 Kafka（仿真可不消费），闭环接口先打通
 */
public class CongestionHotspotSuggestWindowFn
        extends ProcessWindowFunction<DwdTrajPoint, String, String, TimeWindow> {

    /** 复用你碰撞网格：cellSize 默认 = d_min */
    private final AppConfig cfg;

    /** 参数（先写死，后续可以从 run_meta 广播覆盖） */
    private final int hotCntThres;      // cell 内点数阈值：>= 认为热点
    private final int topNHotCells;     // 每帧最多精检多少个热点 cell
    private final double refineDistMul; // 精检距离 = d_min * mul
    private final int maxTargetsPerFrame;

    private final List<int[]> neigh = SpatialGrid.neighbors27();
    private transient ObjectMapper mapper;

    public CongestionHotspotSuggestWindowFn(AppConfig cfg) {
        this(cfg, 4, 20, 1.5, 3);
    }

    public CongestionHotspotSuggestWindowFn(
            AppConfig cfg,
            int hotCntThres,
            int topNHotCells,
            double refineDistMul,
            int maxTargetsPerFrame
    ) {
        this.cfg = cfg;
        this.hotCntThres = hotCntThres;
        this.topNHotCells = topNHotCells;
        this.refineDistMul = refineDistMul;
        this.maxTargetsPerFrame = maxTargetsPerFrame;
    }

    @Override
    public void process(
            String runId,
            Context ctx,
            Iterable<DwdTrajPoint> elements,
            Collector<String> out
    ) throws Exception {

        if (mapper == null) mapper = new ObjectMapper();

        final long frameStart = ctx.window().getStart(); // 用 window start 作为 frame_t_ms（同你的碰撞检测）
        final double cellSize = cfg.collisionDistM;
        if (cellSize <= 0) return;

        final double dWarn = cfg.collisionDistM * refineDistMul;
        final double d2 = dWarn * dWarn;

        // =========================================================
        // 0) 去重：每 drone 只保留 t_ms 最大的一条（同 CollisionHitWindowFn）
        // =========================================================
        Map<String, P> latest = new HashMap<>();
        for (DwdTrajPoint p : elements) {
            if (p == null || p.drone_id == null) continue;
            P cur = latest.get(p.drone_id);
            if (cur == null || p.t_ms > cur.tMs) {
                P n = new P();
                n.runId = p.run_id;
                n.droneId = p.drone_id;
                n.tMs = p.t_ms;
                n.x = p.x; n.y = p.y; n.z = p.z;
                n.speed = p.speed;
                latest.put(p.drone_id, n);
            }
        }
        if (latest.size() < 2) return;

        // =========================================================
        // 1) 建网格：cellKey -> CellBucket(ix,iy,iz, points)
        // =========================================================
        Map<Long, CellBucket> buckets = new HashMap<>();
        for (P fp : latest.values()) {
            int ix = (int) Math.floor(fp.x / cellSize);
            int iy = (int) Math.floor(fp.y / cellSize);
            int iz = (int) Math.floor(fp.z / cellSize);

            long key = SpatialGrid.cellKey(ix, iy, iz);
            CellBucket b = buckets.get(key);
            if (b == null) {
                b = new CellBucket();
                b.key = key;
                b.ix = ix; b.iy = iy; b.iz = iz;
                b.points = new ArrayList<>();
                buckets.put(key, b);
            }
            b.points.add(fp);
        }

        // =========================================================
        // 2) 段1：cell 预聚合 -> 热点筛选（阈值 + TopN）
        // =========================================================
        List<CellBucket> hot = new ArrayList<>();
        for (CellBucket b : buckets.values()) {
            int cnt = b.points == null ? 0 : b.points.size();
            if (cnt >= hotCntThres) hot.add(b);
        }
        if (hot.isEmpty()) return;

        hot.sort((a, b) -> Integer.compare(size(b), size(a)));
        if (hot.size() > topNHotCells) hot = hot.subList(0, topNHotCells);

        // =========================================================
        // 3) 段2：热点精检（同 cell + 邻 cell）-> 风险计分
        //     - 只在热点周围做距离精检，避免全量 O(N^2)
        // =========================================================
        Map<String, Integer> riskScore = new HashMap<>();
        Map<String, Integer> riskPairs = new HashMap<>(); // droneId -> 近距离 pair 次数（可用于 reason 展示）
        Set<String> seenPairs = new HashSet<>();          // 窗口内 pair 去重（热点数有限，不会炸）

        for (CellBucket hc : hot) {
            if (hc.points == null || hc.points.isEmpty()) continue;

            // 枚举热点 cell 的 27 邻域
            for (int[] d : neigh) {
                int nx = hc.ix + d[0];
                int ny = hc.iy + d[1];
                int nz = hc.iz + d[2];
                long nk = SpatialGrid.cellKey(nx, ny, nz);

                CellBucket nb = buckets.get(nk);
                if (nb == null || nb.points == null || nb.points.isEmpty()) continue;

                // 精检：热点 cell 内点 vs 邻 cell 点
                for (P a : hc.points) {
                    for (P b : nb.points) {
                        if (a.droneId.equals(b.droneId)) continue;

                        // pair 去重：用字典序固定 (min|max)
                        String u = a.droneId.compareTo(b.droneId) <= 0 ? a.droneId : b.droneId;
                        String v = a.droneId.compareTo(b.droneId) <= 0 ? b.droneId : a.droneId;
                        String pairKey = u + "|" + v + "|" + frameStart; // 同一帧去重
                        if (!seenPairs.add(pairKey)) continue;

                        double dx = a.x - b.x;
                        double dy = a.y - b.y;
                        double dz = a.z - b.z;
                        double dist2 = dx * dx + dy * dy + dz * dz;
                        if (dist2 <= d2) {
                            // 近距离冲突对：给双方加分
                            inc(riskScore, a.droneId, 1);
                            inc(riskScore, b.droneId, 1);
                            inc(riskPairs, a.droneId, 1);
                            inc(riskPairs, b.droneId, 1);
                        }
                    }
                }
            }
        }

        if (riskScore.isEmpty()) return;

        // =========================================================
        // 4) 生成 suggestion：每帧最多 maxTargetsPerFrame 架
        //     - 先用稳定可解释规则：riskScore TopK -> DELAY_DRONE
        // =========================================================
        List<Map.Entry<String, Integer>> ranked = new ArrayList<>(riskScore.entrySet());
        ranked.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        int emitN = Math.min(maxTargetsPerFrame, ranked.size());
        for (int i = 0; i < emitN; i++) {
            String target = ranked.get(i).getKey();
            int score = ranked.get(i).getValue();
            int pairs = riskPairs.getOrDefault(target, 0);

            // delay 策略：基础 2 帧，风险高再加帧（依然很保守）
            int delayFrames = 2;
            if (score >= 3) delayFrames = 3;
            if (score >= 6) delayFrames = 4;
            long delayMs = delayFrames * cfg.trajFrameMs;

            out.collect(buildSuggestionJson(
                    mapper, runId, frameStart, target,
                    delayMs, delayFrames,
                    score, pairs,
                    hot.size(), cellSize, dWarn
            ));
        }
    }

    // ---------------- helpers ----------------

    private static int size(CellBucket b) {
        return b.points == null ? 0 : b.points.size();
    }

    private static void inc(Map<String, Integer> m, String k, int v) {
        m.put(k, m.getOrDefault(k, 0) + v);
    }

    private static String buildSuggestionJson(
            ObjectMapper mapper,
            String runId,
            long frameTMs,
            String target,
            long delayMs,
            int delayFrames,
            int riskScore,
            int riskPairs,
            int hotCells,
            double cellSizeM,
            double dWarnM
    ) throws Exception {

        long emitMs = System.currentTimeMillis();

        ObjectNode root = mapper.createObjectNode();
        root.put("schema_version", "ctrl_v1");
        root.put("event_type", "control_suggestion");
        root.put("type", "DELAY_DRONE");

        root.put("run_id", runId);
        root.put("frame_t_ms", frameTMs);

        root.putArray("targets").add(target);

        ObjectNode action = root.putObject("action");
        action.put("delay_ms", delayMs);
        action.put("delay_frames", delayFrames);

        ObjectNode reason = root.putObject("reason");
        reason.put("kind", "congestion_refined");
        reason.put("risk_score", riskScore);
        reason.put("risk_pairs", riskPairs);
        reason.put("hot_cells_refined", hotCells);
        reason.put("cell_size_m", cellSizeM);
        reason.put("d_warn_m", dWarnM);

        // 幂等 key：run + frame + type + target（允许 AT_LEAST_ONCE）
        root.put("suggestion_id", runId + "|" + frameTMs + "|DELAY|" + target);
        root.put("emit_ms", emitMs);

        return mapper.writeValueAsString(root);
    }

    /** 窗口内点（比 FramePoint 多一个 speed，避免改你的 model） */
    private static class P {
        String runId;
        String droneId;
        long tMs;
        double x, y, z;
        Double speed;
    }

    /** cell 桶：保存 ix/iy/iz 方便邻域定位（不需要反解 hash key） */
    private static class CellBucket {
        long key;
        int ix, iy, iz;
        List<P> points;
    }
}

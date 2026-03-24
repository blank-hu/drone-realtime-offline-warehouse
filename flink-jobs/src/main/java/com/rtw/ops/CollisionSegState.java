package com.rtw.ops;

import com.rtw.model.CollisionHit;
import com.rtw.model.DwsCollisionEvent;

import java.time.Instant;
import java.time.ZoneOffset;

/**
 * 某一个 (run_id, drone_a, drone_b) 的“碰撞段”运行时状态（segment accumulator）。
 *
 * Doris UNIQUE KEY（主键稳定）：
 *   dt, run_id, drone_a, drone_b, start_t_ms
 *
 * 时间口径（帧语义）：
 *   start_t_ms      = 第一帧 windowStart
 *   last_frame_t_ms = 最近一次命中帧 windowStart
 *   end_t_ms        = last_frame_t_ms + frame_ms（帧末尾）
 */
public class CollisionSegState {

    // ========== key columns（对应 Doris UNIQUE KEY）==========
    public String dt;              // yyyy-MM-dd (UTC)
    public String run_id;
    public String drone_a;         // 归一化后 a <= b
    public String drone_b;         // 归一化后
    public Long start_t_ms;        // 段起点（决定主键）

    // ========== non-key columns ==========
    public String event_id;        // 可追溯：由主键派生
    public Long end_t_ms;          // 段结束（帧末尾）
    public Long duration_ms;

    public Integer frames_cnt;
    public Double min_dist;
    public Double avg_dist;
    public Double d_min;

    public Double start_ax, start_ay, start_az;
    public Double start_bx, start_by, start_bz;
    public Double end_ax, end_ay, end_az;
    public Double end_bx, end_by, end_bz;

    // 追溯维度（与 DwsCollisionEvent 对齐：不含 scenario_id）
    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    public Long update_ms;

    // ========== internal accumulator ==========
    public Long last_frame_t_ms;   // 最近一次命中帧（windowStart）
    public Double sum_dist;        // 用于 avg

    public CollisionSegState() {}

    public static CollisionSegState startFromHit(
            CollisionHit hit,
            String a, String b,                 // 归一化后的 drone_a/drone_b（a<=b）
            double ax, double ay, double az,    // 与 a 对应的坐标
            double bx, double by, double bz,    // 与 b 对应的坐标
            long frameMs
    ) {
        CollisionSegState s = new CollisionSegState();

        s.run_id = hit.run_id;
        s.drone_a = a;
        s.drone_b = b;

        long start = hit.frame_t_ms;
        s.start_t_ms = start;
        s.last_frame_t_ms = start;

        // 段结束：帧末尾
        s.end_t_ms = start + frameMs;
        s.duration_ms = frameMs;

        s.dt = dtUtc(start);
        s.event_id = a + "|" + b + "|collision|" + start;

        s.frames_cnt = 1;
        s.min_dist = hit.dist;
        s.sum_dist = hit.dist;
        s.avg_dist = hit.dist;

        s.d_min = hit.d_min;

        s.start_ax = ax; s.start_ay = ay; s.start_az = az;
        s.start_bx = bx; s.start_by = by; s.start_bz = bz;

        s.end_ax = ax; s.end_ay = ay; s.end_az = az;
        s.end_bx = bx; s.end_by = by; s.end_bz = bz;

        // 追溯维度：从 hit 带
        s.strategy_id = hit.strategy_id;
        s.strategy_version = hit.strategy_version;
        s.param_set_id = hit.param_set_id;
        s.seed = hit.seed;

        s.update_ms = System.currentTimeMillis();
        return s;
    }

    /** 用新 hit 更新当前段（hit.frame_t_ms 必须单调递增） */
    public void updateWithHit(CollisionHit hit,
                              double ax, double ay, double az,
                              double bx, double by, double bz,
                              long frameMs) {
        long f = hit.frame_t_ms;
        this.last_frame_t_ms = f;

        // end=帧末尾
        this.end_t_ms = f + frameMs;

        // 计数/聚合
        this.frames_cnt = (this.frames_cnt == null ? 0 : this.frames_cnt) + 1;

        if (this.min_dist == null) this.min_dist = hit.dist;
        else this.min_dist = Math.min(this.min_dist, hit.dist);

        // 2. 【新增】更新阈值信息 (Fix: 防止配置变更后出现 min_dist > d_min 的悖论)
        this.d_min = hit.d_min;

        if (this.sum_dist == null) this.sum_dist = 0.0;
        this.sum_dist += hit.dist;

        this.avg_dist = (this.frames_cnt != null && this.frames_cnt > 0)
                ? (this.sum_dist / this.frames_cnt)
                : null;

        // 末尾坐标
        this.end_ax = ax; this.end_ay = ay; this.end_az = az;
        this.end_bx = bx; this.end_by = by; this.end_bz = bz;

        // duration
        if (this.start_t_ms != null && this.end_t_ms != null) {
            this.duration_ms = Math.max(0L, this.end_t_ms - this.start_t_ms);
        }

        this.update_ms = System.currentTimeMillis();
    }

    public DwsCollisionEvent toEvent() {
        DwsCollisionEvent e = new DwsCollisionEvent();
        e.dt = this.dt;

        e.run_id = this.run_id;
        e.drone_a = this.drone_a;
        e.drone_b = this.drone_b;
        e.start_t_ms = this.start_t_ms;

        e.event_id = this.event_id;
        e.end_t_ms = this.end_t_ms;
        e.duration_ms = this.duration_ms;

        e.frames_cnt = this.frames_cnt;
        e.min_dist = this.min_dist;
        e.avg_dist = this.avg_dist;
        e.d_min = this.d_min;

        e.start_ax = this.start_ax; e.start_ay = this.start_ay; e.start_az = this.start_az;
        e.start_bx = this.start_bx; e.start_by = this.start_by; e.start_bz = this.start_bz;
        e.end_ax = this.end_ax; e.end_ay = this.end_ay; e.end_az = this.end_az;
        e.end_bx = this.end_bx; e.end_by = this.end_by; e.end_bz = this.end_bz;

        e.strategy_id = this.strategy_id;
        e.strategy_version = this.strategy_version;
        e.param_set_id = this.param_set_id;
        e.seed = this.seed;

        e.update_ms = System.currentTimeMillis();
        return e;
    }

    private static String dtUtc(long tMs) {
        return Instant.ofEpochMilli(tMs).atZone(ZoneOffset.UTC).toLocalDate().toString();
    }
}

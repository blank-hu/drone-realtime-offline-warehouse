package com.rtw.model;

/**
 * dws_run_quality_v1 的一行（run 级汇总）
 * - dt：UTC 日期字符串 "yyyy-MM-dd"（Doris DATE 可接收）
 * - top_drones_json：TopN 明细用 JSON 字符串存（看板/排查方便）
 *
 * 新增：
 * - is_final：0=运行中快照（覆盖更新）；1=最终定稿（覆盖最后一次快照）
 * - snapshot_ms：本次输出的时间（快照/最终都会填）
 * - score_weights_json：TopN score 的权重口径，可追溯
 */
public class DwsRunQuality {
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

    public Long overspeed_cnt;
    public Long overacc_cnt;
    public Long teleport_cnt;

    public Integer drone_cnt;
    public String top_drones_json;

    public Long update_ms;

    // ===== 新增字段（对应 Doris 新增列）=====
    public Integer is_final;           // 0/1
    public Long snapshot_ms;           // 本次输出时刻
    public String score_weights_json;  // {"w_os":2,"w_oa":3,"w_tp":10}
}

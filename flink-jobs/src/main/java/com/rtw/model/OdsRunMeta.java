package com.rtw.model;

/** Kafka topic: ods_run_meta 的事件 */
public class OdsRunMeta {
    public String schema_version;
    public String event_type;

    public String scenario_id;
    public String run_id;

    public Long start_ms;

    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    /** 注意：这里是一个 JSON 字符串，比如 {"v_max":3.0,"dt":0.04,...} */
    public String param_json;

    // end/update 字段（可能没有）
    public Long end_ms;
    public String status;
    public Long duration_ms;

    // 可能存在的附加字段（producer 可能带）
    public Integer drone_count;
    public Long points_sent;
}

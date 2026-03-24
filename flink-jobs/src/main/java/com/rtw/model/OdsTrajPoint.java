package com.rtw.model;


public class OdsTrajPoint {
    public String schema_version;
    public String event_type;

    public String scenario_id;
    public String run_id;
    public String drone_id;

    public long t_ms;   // epoch ms
    public int seq;

    public double x;
    public double y;
    public double z;

    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    // ===== run_end_marker 可选字段（普通点为 null） =====
    public String status;
    public Integer drone_count;
    public Long points_sent;

    public String keyRunDrone() { return run_id + "|" + drone_id; }
}

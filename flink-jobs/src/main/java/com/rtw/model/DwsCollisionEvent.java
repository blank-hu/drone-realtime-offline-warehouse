package com.rtw.model;

public class DwsCollisionEvent {
    public String dt;

    public String run_id;
    public String drone_a;
    public String drone_b;
    public Long start_t_ms;

    public String event_id;
    public Long end_t_ms;
    public Long duration_ms;

    public Integer frames_cnt;
    public Double min_dist;
    public Double avg_dist;
    public Double d_min;

    public Double start_ax, start_ay, start_az;
    public Double start_bx, start_by, start_bz;
    public Double end_ax, end_ay, end_az;
    public Double end_bx, end_by, end_bz;

    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    public Long update_ms;
}

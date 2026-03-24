package com.rtw.model;

public class DwsOverspeedEvent {
    public String dt;
    public String run_id;
    public String event_id;

    public String drone_id;

    public Integer start_seq;
    public Integer end_seq;
    public Long start_t_ms;
    public Long end_t_ms;
    public Long duration_ms;

    public Integer points_cnt;
    public Double max_speed;
    public Double avg_speed;

    public Double start_x, start_y, start_z;
    public Double end_x, end_y, end_z;

    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    public Long update_ms;
}

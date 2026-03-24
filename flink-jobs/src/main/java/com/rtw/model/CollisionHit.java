package com.rtw.model;

public class CollisionHit {
    public String run_id;
    public long frame_t_ms;

    public String drone_a;
    public String drone_b;

    public double dist;
    public double d_min;

    public double ax, ay, az;
    public double bx, by, bz;

    public String scenario_id;
    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    public long update_ms;
    public CollisionHit() {}
}
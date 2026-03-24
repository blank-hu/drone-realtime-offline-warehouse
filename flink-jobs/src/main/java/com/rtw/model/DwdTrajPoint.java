package com.rtw.model;


/** 写入 dwd_traj_point_v1 的行对象（dt 用 yyyy-MM-dd 字符串传 Stream Load） */
public class DwdTrajPoint {
    public String dt;

    public String run_id;
    public String drone_id;
    public int seq;

    public String scenario_id;
    public long t_ms;

    public double x, y, z;

    public Long dt_ms;
    public Double dx, dy, dz;
    public Double speed;
    public Double acc;

    public Integer is_time_back;
    public Integer is_teleport;
    public Integer is_overspeed;
    public Integer is_overacc;
    public Integer is_stuck;

    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;
}

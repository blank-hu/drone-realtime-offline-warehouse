package com.rtw.model;

public class FramePoint {
    public String runId;
    public String droneId;
    public long tMs;
    public double x, y, z;

    // 追溯维度（可选，但建议带）
    public String scenarioId;
    public String strategyId;
    public String strategyVersion;
    public String paramSetId;
    public Integer seed;

    public FramePoint() {}
}
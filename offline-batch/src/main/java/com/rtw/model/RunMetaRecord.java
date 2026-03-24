package com.rtw.model;

public class RunMetaRecord {
    private String runId;
    private String scenarioId;
    private String strategyId;
    private String strategyVersion;
    private String paramJson;
    private String paramSetId;
    private Integer seed;
    private Long startMs;
    private Long endMs;
    private String status;
    private Integer droneCount;
    private Long durationMs;

    // 可直接展开的阈值字段，方便 DWD join 后直接使用
    private Double vMax;
    private Double aMax;
    private Double teleportDistM;

    public RunMetaRecord() {}

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }

    public String getScenarioId() { return scenarioId; }
    public void setScenarioId(String scenarioId) { this.scenarioId = scenarioId; }

    public String getStrategyId() { return strategyId; }
    public void setStrategyId(String strategyId) { this.strategyId = strategyId; }

    public String getStrategyVersion() { return strategyVersion; }
    public void setStrategyVersion(String strategyVersion) { this.strategyVersion = strategyVersion; }

    public String getParamJson() { return paramJson; }
    public void setParamJson(String paramJson) { this.paramJson = paramJson; }

    public String getParamSetId() { return paramSetId; }
    public void setParamSetId(String paramSetId) { this.paramSetId = paramSetId; }

    public Integer getSeed() { return seed; }
    public void setSeed(Integer seed) { this.seed = seed; }

    public Long getStartMs() { return startMs; }
    public void setStartMs(Long startMs) { this.startMs = startMs; }

    public Long getEndMs() { return endMs; }
    public void setEndMs(Long endMs) { this.endMs = endMs; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Integer getDroneCount() { return droneCount; }
    public void setDroneCount(Integer droneCount) { this.droneCount = droneCount; }

    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

    public Double getVMax() { return vMax; }
    public void setVMax(Double vMax) { this.vMax = vMax; }

    public Double getAMax() { return aMax; }
    public void setAMax(Double aMax) { this.aMax = aMax; }

    public Double getTeleportDistM() { return teleportDistM; }
    public void setTeleportDistM(Double teleportDistM) { this.teleportDistM = teleportDistM; }
}
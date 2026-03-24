package com.rtw.model;

public class RunParams {
    private Double vMax;
    private Double aMax;
    private Double teleportDistM;

    public RunParams() {}

    public RunParams(Double vMax, Double aMax, Double teleportDistM) {
        this.vMax = vMax;
        this.aMax = aMax;
        this.teleportDistM = teleportDistM;
    }

    public Double getVMax() { return vMax; }
    public void setVMax(Double vMax) { this.vMax = vMax; }

    public Double getAMax() { return aMax; }
    public void setAMax(Double aMax) { this.aMax = aMax; }

    public Double getTeleportDistM() { return teleportDistM; }
    public void setTeleportDistM(Double teleportDistM) { this.teleportDistM = teleportDistM; }
}
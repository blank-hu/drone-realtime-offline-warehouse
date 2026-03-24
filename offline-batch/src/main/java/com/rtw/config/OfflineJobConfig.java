package com.rtw.config;

public class OfflineJobConfig {

    public static final String PROJECT_DIR = "D:/warehouse/offline-batch";
    public static final String WAREHOUSE_DIR = "file:///D:/warehouse/offline-batch/spark-warehouse";
    public static final String DATA_DIR = PROJECT_DIR + "/data";

    public static final String INPUT_RUN_NAME = "run_001";
    public static final String INPUT_RUN_DIR = DATA_DIR + "/" + INPUT_RUN_NAME;

    public static final String INPUT_TRAJ_JSON = INPUT_RUN_DIR + "/trajectories_takeoff.json";
    public static final String INPUT_META_YAML = INPUT_RUN_DIR + "/meta.yaml";

    public static final String DATA_WAREHOUSE_DIR = DATA_DIR + "/warehouse";

    public static final String ODS_RUN_META_PATH = DATA_WAREHOUSE_DIR + "/ods_run_meta_di";
    public static final String ODS_TRAJ_POINT_PATH = DATA_WAREHOUSE_DIR + "/ods_traj_point_di";
    public static final String DWD_TRAJ_POINT_PATH = DATA_WAREHOUSE_DIR + "/dwd_traj_point_detail_di";
    public static final String DWS_RUN_QUALITY_PATH = DATA_WAREHOUSE_DIR + "/dws_run_quality_di";
    public static final String DWS_OVERSPEED_EVENT_PATH = DATA_WAREHOUSE_DIR + "/dws_overspeed_event_di";
    public static final String DWS_COLLISION_EVENT_PATH = DATA_WAREHOUSE_DIR + "/dws_collision_event_di";

    public static final String DB_NAME = "drone_dw";

    public static final int DWS_TOP_N = 5;
    public static final int SCORE_W_OS = 5;
    public static final int SCORE_W_OA = 3;
    public static final int SCORE_W_TP = 8;

    /** 与实时链路对齐：40ms 一帧 */
    public static final long TRAJ_FRAME_MS = 40L;

    /** 与实时碰撞阈值对齐 */
    public static final double COLLISION_DIST_M = 3.0d;

    // =========================
    // Doris 对账配置
    // =========================
    public static final boolean ENABLE_RT_RECONCILE = false;

    /** Doris FE MySQL 协议 JDBC 地址 */
    public static final String DORIS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030/rtw";

    public static final String DORIS_USER = "root";
    public static final String DORIS_PASSWORD = "";

    /** 实时链路写入的 Doris 数据库名 */
    public static final String DORIS_DB = "rtw";

    private OfflineJobConfig() {
    }
}
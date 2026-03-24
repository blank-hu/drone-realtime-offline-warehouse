package com.rtw.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AppConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // Kafka
    public final String kafkaBootstrap;
    public final String topicTrajPoint;
    public final String groupId;

    // Doris
    public final String dorisFeHost;
    public final int dorisFeHttpPort;
    public final String dorisUser;
    public final String dorisPassword;
    public final String dorisDb;
    public final String dorisTableDwd;

    // Flink
    public final int flinkParallelism;
    public final long watermarkOooMs;

    // fallback thresholds（会被 run_meta.param_json 覆盖）
    public final double teleportDistM;
    public final double vMax;
    public final double aMax;

    // === 【新增】碰撞检测安全距离（米） ===
    public final double collisionDistM;

    // 轨迹点帧间隔（ms），run_end_marker 会使用 end_ms + trajFrameMs
    public final long trajFrameMs;


    // ===== DWS（新增）=====
    /** FINAL_ONLY / INCR_AND_FINAL */
    public final String dwsEmitMode;
    /** 增量快照输出间隔（ms），仅 INCR_AND_FINAL 生效 */
    public final long dwsEmitIntervalMs;
    /** end 到来后延迟定稿（ms） */
    public final long dwsFinalGraceMs;
    /** TopN 大小 */
    public final int dwsTopN;
    /** score 权重 */
    public final int scoreWOs; // overspeed
    public final int scoreWOa; // overacc
    public final int scoreWTp; // teleport

    private AppConfig(
            String kafkaBootstrap, String topicTrajPoint, String groupId,
            String dorisFeHost, int dorisFeHttpPort, String dorisUser, String dorisPassword,
            String dorisDb, String dorisTableDwd,
            int flinkParallelism, long watermarkOooMs,
            double teleportDistM, double vMax, double aMax,
            long trajFrameMs,
            String dwsEmitMode, long dwsEmitIntervalMs, long dwsFinalGraceMs,
            int dwsTopN, int scoreWOs, int scoreWOa, int scoreWTp,
            double collisionDistM
    ) {
        this.collisionDistM = collisionDistM;
        this.kafkaBootstrap = kafkaBootstrap;
        this.topicTrajPoint = topicTrajPoint;
        this.groupId = groupId;

        this.dorisFeHost = dorisFeHost;
        this.dorisFeHttpPort = dorisFeHttpPort;
        this.dorisUser = dorisUser;
        this.dorisPassword = dorisPassword;
        this.dorisDb = dorisDb;
        this.dorisTableDwd = dorisTableDwd;

        this.flinkParallelism = flinkParallelism;
        this.watermarkOooMs = watermarkOooMs;

        this.teleportDistM = teleportDistM;
        this.vMax = vMax;
        this.aMax = aMax;

        this.trajFrameMs = trajFrameMs;


        this.dwsEmitMode = dwsEmitMode;
        this.dwsEmitIntervalMs = dwsEmitIntervalMs;
        this.dwsFinalGraceMs = dwsFinalGraceMs;
        this.dwsTopN = dwsTopN;

        this.scoreWOs = scoreWOs;
        this.scoreWOa = scoreWOa;
        this.scoreWTp = scoreWTp;
    }

    public static AppConfig fromArgsEnv(String[] args) {
        Map<String, String> kv = parseArgs(args);
        Map<String, String> env = System.getenv();


        java.util.function.BiFunction<String, String, String> get = (k, d) -> {
            String v = kv.get(k);
            if (v != null && !v.isEmpty()) return v;
            v = env.get(k);
            if (v != null && !v.isEmpty()) return v;
            return d;
        };

        // Kafka
        String kafkaBootstrap = get.apply("KAFKA_BOOTSTRAP", "192.168.85.128:29092:29092");
        String topicTrajPoint = get.apply("KAFKA_TOPIC_TRAJ_POINT", "ods_traj_point");
        String groupId = get.apply("KAFKA_GROUP_ID", "rtw-dwd-v1");

        // Doris
        String feHost = get.apply("DORIS_FE_HOST", "192.168.85.128");
        int fePort = Integer.parseInt(get.apply("DORIS_FE_HTTP_PORT", "8030"));
        String user = get.apply("DORIS_USER", "flink");
        String pwd = get.apply("DORIS_PASSWORD", "Flink@123456");
        String db = get.apply("DORIS_DB", "uav_dw");
        String table = get.apply("DORIS_TABLE_DWD", "dwd_traj_point_v1");

        // Flink
        int parallelism = Integer.parseInt(get.apply("FLINK_PARALLELISM", "1"));
        long ooo = Long.parseLong(get.apply("WATERMARK_OOO_MS", "200"));

        // fallback thresholds
        double teleport = Double.parseDouble(get.apply("TELEPORT_DIST_M", "20.0"));
        double vMax = Double.parseDouble(get.apply("V_MAX", "8.0"));
        double aMax = Double.parseDouble(get.apply("A_MAX", "6.0"));

        long trajFrameMs = Long.parseLong(get.apply("TRAJ_FRAME_MS", "60"));


        // ===== DWS（新增）=====
        String emitMode = get.apply("DWS_EMIT_MODE", "INCR_AND_FINAL"); // 默认开快照
        long emitInterval = Long.parseLong(get.apply("DWS_EMIT_INTERVAL_MS", "10000"));
        long finalGrace = Long.parseLong(get.apply("DWS_FINAL_GRACE_MS", "5000"));
        int topN = Integer.parseInt(get.apply("DWS_TOP_N", "5"));

        int wOs = Integer.parseInt(get.apply("SCORE_W_OS", "2"));
        int wOa = Integer.parseInt(get.apply("SCORE_W_OA", "3"));
        int wTp = Integer.parseInt(get.apply("SCORE_W_TP", "10"));

        // === 【新增】解析配置，默认 5.0 米 ===
        double collisionDist = Double.parseDouble(get.apply("COLLISION_DIST_M", "3.0"));

        return new AppConfig(
                kafkaBootstrap, topicTrajPoint, groupId,
                feHost, fePort, user, pwd, db, table,
                parallelism, ooo,
                teleport, vMax, aMax,
                trajFrameMs,
                emitMode, emitInterval, finalGrace,
                topN, wOs, wOa, wTp,
                collisionDist
        );
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        if (args == null) return m;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) continue;
            String key = a.substring(2);
            String val = "true";
            if (key.contains("=")) {
                String[] sp = key.split("=", 2);
                key = sp[0];
                val = sp[1];
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                val = args[++i];
            }
            m.put(key, val);
        }
        return m;
    }
}

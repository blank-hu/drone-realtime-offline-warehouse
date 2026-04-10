package com.rtw.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class AppConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // =========================
    // Kafka
    // =========================
    public final String kafkaBootstrap;
    public final String topicTrajPoint;
    public final String topicRunMeta;
    public final String groupId;

    // =========================
    // Doris
    // =========================
    public final String dorisFeHost;
    public final int dorisFeHttpPort;
    public final String dorisUser;
    public final String dorisPassword;
    public final String dorisDb;

    /** 仅异常点明细表 */
    public final String dorisTableRiskPoint;
    /** run 级质量汇总表 */
    public final String dorisTableRunQuality;
    /** 连续超速事件段表 */
    public final String dorisTableOverspeedEvent;
    /** 碰撞事件段表 */
    public final String dorisTableCollisionEvent;

    // =========================
    // Flink
    // =========================
    public final int flinkParallelism;
    /** 允许乱序时间（ms） */
    public final long watermarkOooMs;

    // =========================
    // fallback thresholds
    // 若 run_meta.param_json 未显式提供，则使用这里的默认值
    // =========================
    public final double teleportDistM;
    public final double vMax;
    public final double aMax;
    public final double collisionDistM;

    /** 轨迹帧间隔（ms） */
    public final long trajFrameMs;

    // =========================
    // DWS
    // =========================
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
            String kafkaBootstrap,
            String topicTrajPoint,
            String topicRunMeta,
            String groupId,

            String dorisFeHost,
            int dorisFeHttpPort,
            String dorisUser,
            String dorisPassword,
            String dorisDb,
            String dorisTableRiskPoint,
            String dorisTableRunQuality,
            String dorisTableOverspeedEvent,
            String dorisTableCollisionEvent,

            int flinkParallelism,
            long watermarkOooMs,

            double teleportDistM,
            double vMax,
            double aMax,
            double collisionDistM,
            long trajFrameMs,

            String dwsEmitMode,
            long dwsEmitIntervalMs,
            long dwsFinalGraceMs,
            int dwsTopN,
            int scoreWOs,
            int scoreWOa,
            int scoreWTp
    ) {
        this.kafkaBootstrap = kafkaBootstrap;
        this.topicTrajPoint = topicTrajPoint;
        this.topicRunMeta = topicRunMeta;
        this.groupId = groupId;

        this.dorisFeHost = dorisFeHost;
        this.dorisFeHttpPort = dorisFeHttpPort;
        this.dorisUser = dorisUser;
        this.dorisPassword = dorisPassword;
        this.dorisDb = dorisDb;
        this.dorisTableRiskPoint = dorisTableRiskPoint;
        this.dorisTableRunQuality = dorisTableRunQuality;
        this.dorisTableOverspeedEvent = dorisTableOverspeedEvent;
        this.dorisTableCollisionEvent = dorisTableCollisionEvent;

        this.flinkParallelism = flinkParallelism;
        this.watermarkOooMs = watermarkOooMs;

        this.teleportDistM = teleportDistM;
        this.vMax = vMax;
        this.aMax = aMax;
        this.collisionDistM = collisionDistM;
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

        // =========================
        // Kafka
        // =========================
        String kafkaBootstrap = get.apply("KAFKA_BOOTSTRAP", "192.168.85.128:29092");
        String topicTrajPoint = get.apply("KAFKA_TOPIC_TRAJ_POINT", "ods_traj_point");
        String topicRunMeta = get.apply("KAFKA_TOPIC_RUN_META", "ods_run_meta");
        String groupId = get.apply("KAFKA_GROUP_ID", "rtw-dwd-v1");

        // =========================
        // Doris
        // =========================
        String feHost = get.apply("DORIS_FE_HOST", "192.168.85.128");
        int fePort = parseInt(get.apply("DORIS_FE_HTTP_PORT", "8030"), 8030);
        String user = get.apply("DORIS_USER", "root");
        String pwd = get.apply("DORIS_PASSWORD", "");
        String db = get.apply("DORIS_DB", "uav_dw");

        String riskPointTable = get.apply("DORIS_TABLE_RISK_POINT", "dwd_risk_point_recent");
        String runQualityTable = get.apply("DORIS_TABLE_RUN_QUALITY", "dws_run_quality_v1");
        String overspeedEventTable = get.apply("DORIS_TABLE_OVERSPEED_EVENT", "dws_overspeed_event_v1");
        String collisionEventTable = get.apply("DORIS_TABLE_COLLISION_EVENT", "dws_collision_event_v1");

        // =========================
        // Flink
        // =========================
        int parallelism = parseInt(get.apply("FLINK_PARALLELISM", "1"), 1);
        long trajFrameMs = parseLong(get.apply("TRAJ_FRAME_MS", "40"), 40L);
        long watermarkOooMs = parseLong(
                get.apply("WATERMARK_OOO_MS", String.valueOf(Math.max(1L, trajFrameMs * 2L))),
                Math.max(1L, trajFrameMs * 2L)
        );

        // =========================
        // fallback thresholds
        // 与离线默认口径统一
        // =========================
        double teleportDistM = parseDouble(get.apply("TELEPORT_DIST_M", "10.0"), 10.0d);
        double vMax = parseDouble(get.apply("V_MAX", "8.0"), 8.0d);
        double aMax = parseDouble(get.apply("A_MAX", "2.0"), 2.0d);
        double collisionDistM = parseDouble(get.apply("COLLISION_DIST_M", "3.0"), 3.0d);

        // =========================
        // DWS
        // =========================
        String emitMode = normalizeEmitMode(get.apply("DWS_EMIT_MODE", "INCR_AND_FINAL"));
        long emitInterval = parseLong(get.apply("DWS_EMIT_INTERVAL_MS", "10000"), 10000L);
        long finalGrace = parseLong(get.apply("DWS_FINAL_GRACE_MS", "5000"), 5000L);
        int topN = parseInt(get.apply("DWS_TOP_N", "5"), 5);

        // 与离线 OfflineJobConfig 保持一致
        int wOs = parseInt(get.apply("SCORE_W_OS", "5"), 5);
        int wOa = parseInt(get.apply("SCORE_W_OA", "3"), 3);
        int wTp = parseInt(get.apply("SCORE_W_TP", "8"), 8);

        return new AppConfig(
                kafkaBootstrap,
                topicTrajPoint,
                topicRunMeta,
                groupId,

                feHost,
                fePort,
                user,
                pwd,
                db,
                riskPointTable,
                runQualityTable,
                overspeedEventTable,
                collisionEventTable,

                parallelism,
                watermarkOooMs,

                teleportDistM,
                vMax,
                aMax,
                collisionDistM,
                trajFrameMs,

                emitMode,
                emitInterval,
                finalGrace,
                topN,
                wOs,
                wOa,
                wTp
        );
    }

    private static String normalizeEmitMode(String mode) {
        if (mode == null || mode.trim().isEmpty()) {
            return "INCR_AND_FINAL";
        }
        String m = mode.trim().toUpperCase(Locale.ROOT);
        if (!"FINAL_ONLY".equals(m) && !"INCR_AND_FINAL".equals(m)) {
            return "INCR_AND_FINAL";
        }
        return m;
    }

    private static int parseInt(String s, int def) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return def;
        }
    }

    private static long parseLong(String s, long def) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return def;
        }
    }

    private static double parseDouble(String s, double def) {
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return def;
        }
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
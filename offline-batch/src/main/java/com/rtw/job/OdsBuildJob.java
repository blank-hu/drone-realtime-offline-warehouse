package com.rtw.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rtw.config.OfflineJobConfig;
import com.rtw.model.RunMetaRecord;
import com.rtw.util.HiveTableManager;
import com.rtw.util.YamlMetaLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * ODS 构建：
 * 1) meta.yaml -> ods_run_meta_di
 * 2) trajectories_takeoff.json(tracks) -> ods_traj_point_di
 *
 * run_id 规则：
 * - 优先使用 YAML 中的 run_id；
 * - 如果 YAML 不带 run_id，则用目录名推导，例如 data/run_001/meta.yaml -> run_001。
 */
public class OdsBuildJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long EPOCH_THRESHOLD_MS = 1_000_000_000_000L;

    public void run(SparkSession spark) {
        HiveTableManager.initDatabase(spark);

        RunMetaRecord meta = YamlMetaLoader.load(OfflineJobConfig.INPUT_META_YAML);
        if (meta.getRunId() == null || meta.getRunId().isBlank()) {
            throw new IllegalArgumentException("无法确定 run_id：YAML 未提供，且目录名推导失败");
        }

        long baseEpochMs = resolveBaseEpochMs(meta);
        List<TrajPointRecord> records = loadTrajectoryRecords(OfflineJobConfig.INPUT_TRAJ_JSON, meta, baseEpochMs);
        if (records.isEmpty()) {
            throw new IllegalStateException("轨迹文件中没有任何点: " + OfflineJobConfig.INPUT_TRAJ_JSON);
        }

        enrichMetaByTrajectory(meta, records);

        Dataset<Row> metaDf = spark.createDataFrame(Collections.singletonList(meta), RunMetaRecord.class)
                .select(
                        col("runId").alias("run_id"),
                        col("scenarioId").alias("scenario_id"),
                        col("strategyId").alias("strategy_id"),
                        col("strategyVersion").alias("strategy_version"),
                        col("paramJson").alias("param_json"),
                        col("paramSetId").alias("param_set_id"),
                        col("seed").alias("seed"),
                        col("startMs").alias("start_ms"),
                        col("endMs").alias("end_ms"),
                        col("status").alias("status"),
                        col("droneCount").alias("drone_count"),
                        col("durationMs").alias("duration_ms"),
                        col("vMax").alias("v_max"),
                        col("aMax").alias("a_max"),
                        col("teleportDistM").alias("teleport_dist_m")
                );

        Dataset<Row> trajDf = spark.createDataFrame(records, TrajPointRecord.class)
                .select(
                        col("run_id"),
                        col("drone_id"),
                        col("seq"),
                        col("t_ms"),
                        col("x"),
                        col("y"),
                        col("z"),
                        col("vx"),
                        col("vy"),
                        col("vz"),
                        col("ax"),
                        col("ay"),
                        col("az"),
                        col("status"),
                        col("scenario_id"),
                        col("strategy_id"),
                        col("strategy_version"),
                        col("param_set_id"),
                        col("seed"),
                        col("point_key")
                );

        validateTrajectoryDf(trajDf);

        System.out.println("===== ods_run_meta_di schema =====");
        metaDf.printSchema();
        metaDf.show(false);

        System.out.println("===== ods_traj_point_di schema =====");
        trajDf.printSchema();
        trajDf.orderBy("drone_id", "seq").show(50, false);

        metaDf.write().mode(SaveMode.Overwrite).parquet(OfflineJobConfig.ODS_RUN_META_PATH);
        trajDf.write().mode(SaveMode.Overwrite).parquet(OfflineJobConfig.ODS_TRAJ_POINT_PATH);

        HiveTableManager.createOdsRunMetaTable(spark);
        HiveTableManager.createOdsTrajPointTable(spark);

        spark.sql("select * from " + OfflineJobConfig.DB_NAME + ".ods_run_meta_di").show(false);
        spark.sql("select * from " + OfflineJobConfig.DB_NAME + ".ods_traj_point_di order by drone_id, seq limit 50").show(false);
    }

    private static List<TrajPointRecord> loadTrajectoryRecords(String jsonPath, RunMetaRecord meta, long baseEpochMs) {
        try {
            JsonNode root = MAPPER.readTree(new File(jsonPath));
            JsonNode tracksNode;
            if (root.has("tracks")) {
                tracksNode = root.get("tracks");
            } else if (root.has("drones")) {
                tracksNode = root.get("drones");
            } else {
                tracksNode = root;
            }

            List<TrajPointRecord> out = new ArrayList<>();
            if (tracksNode.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = tracksNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String droneId = entry.getKey();
                    JsonNode arr = entry.getValue();
                    if (arr == null || !arr.isArray()) {
                        continue;
                    }
                    for (int i = 0; i < arr.size(); i++) {
                        out.add(buildRecord(meta, baseEpochMs, droneId, i, arr.get(i)));
                    }
                }
            } else if (tracksNode.isArray()) {
                for (int i = 0; i < tracksNode.size(); i++) {
                    JsonNode p = tracksNode.get(i);
                    String droneId = p.hasNonNull("drone_id") ? p.get("drone_id").asText() : "drone_0";
                    int seq = p.hasNonNull("seq") ? p.get("seq").asInt() : i;
                    out.add(buildRecord(meta, baseEpochMs, droneId, seq, p));
                }
            } else {
                throw new IllegalArgumentException("不支持的轨迹 JSON 格式: " + jsonPath);
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("读取轨迹文件失败: " + jsonPath, e);
        }
    }

    private static TrajPointRecord buildRecord(RunMetaRecord meta, long baseEpochMs, String droneId, int seq, JsonNode p) {
        TrajPointRecord r = new TrajPointRecord();
        r.setRun_id(meta.getRunId());
        r.setDrone_id(droneId);
        r.setSeq(seq);

        long rawTms;
        if (p.hasNonNull("t_ms")) {
            rawTms = p.get("t_ms").asLong();
        } else if (p.hasNonNull("t")) {
            rawTms = Math.round(p.get("t").asDouble() * 1000.0);
        } else {
            rawTms = seq * 40L;
        }

        if (rawTms >= EPOCH_THRESHOLD_MS) {
            r.setT_ms(rawTms);
        } else {
            r.setT_ms(baseEpochMs + rawTms);
        }

        r.setX(asDouble(p, "x", 0.0));
        r.setY(asDouble(p, "y", 0.0));
        r.setZ(asDouble(p, "z", 0.0));
        r.setVx(asNullableDouble(p, "vx"));
        r.setVy(asNullableDouble(p, "vy"));
        r.setVz(asNullableDouble(p, "vz"));
        r.setAx(asNullableDouble(p, "ax"));
        r.setAy(asNullableDouble(p, "ay"));
        r.setAz(asNullableDouble(p, "az"));
        r.setStatus(p.hasNonNull("status") ? p.get("status").asText() : null);
        r.setScenario_id(meta.getScenarioId());
        r.setStrategy_id(meta.getStrategyId());
        r.setStrategy_version(meta.getStrategyVersion());
        r.setParam_set_id(meta.getParamSetId());
        r.setSeed(meta.getSeed());
        r.setPoint_key(meta.getRunId() + "|" + droneId + "|" + seq);
        return r;
    }

    private static long resolveBaseEpochMs(RunMetaRecord meta) {
        Long startMs = meta.getStartMs();
        if (hasEpochMs(startMs)) {
            return startMs;
        }
        long now = System.currentTimeMillis();
        return now - Math.floorMod(now, 1000L);
    }

    private static boolean hasEpochMs(Long value) {
        return value != null && value >= EPOCH_THRESHOLD_MS;
    }

    private static void enrichMetaByTrajectory(RunMetaRecord meta, List<TrajPointRecord> records) {
        long minT = Long.MAX_VALUE;
        long maxT = Long.MIN_VALUE;
        java.util.Set<String> droneIds = new java.util.HashSet<>();

        for (TrajPointRecord r : records) {
            minT = Math.min(minT, r.getT_ms());
            maxT = Math.max(maxT, r.getT_ms());
            droneIds.add(r.getDrone_id());
        }

        if ((meta.getDroneCount() == null || meta.getDroneCount() <= 0) && !droneIds.isEmpty()) {
            meta.setDroneCount(droneIds.size());
        }
        if (!hasEpochMs(meta.getStartMs()) && minT != Long.MAX_VALUE) {
            meta.setStartMs(minT);
        }
        if (!hasEpochMs(meta.getEndMs()) && maxT != Long.MIN_VALUE) {
            meta.setEndMs(maxT);
        }
        if (hasEpochMs(meta.getStartMs()) && hasEpochMs(meta.getEndMs()) && meta.getEndMs() < meta.getStartMs() && maxT != Long.MIN_VALUE) {
            meta.setEndMs(maxT);
        }
        if ((meta.getDurationMs() == null || meta.getDurationMs() <= 0)
                && meta.getStartMs() != null && meta.getEndMs() != null) {
            meta.setDurationMs(Math.max(0L, meta.getEndMs() - meta.getStartMs()));
        }
        if (meta.getStatus() == null) {
            meta.setStatus("success");
        }
    }

    private static void validateTrajectoryDf(Dataset<Row> trajDf) {
        List<String> required = java.util.Arrays.asList("run_id", "drone_id", "seq", "t_ms", "x", "y", "z", "point_key");
        List<String> cols = java.util.Arrays.asList(trajDf.columns());
        for (String c : required) {
            if (!cols.contains(c)) {
                throw new IllegalStateException("ods_traj_point_di 缺少必要字段: " + c);
            }
        }
    }

    private static Double asDouble(JsonNode p, String key, Double defaultVal) {
        return p.hasNonNull(key) ? p.get(key).asDouble() : defaultVal;
    }

    private static Double asNullableDouble(JsonNode p, String key) {
        return p.hasNonNull(key) ? p.get(key).asDouble() : null;
    }

    public static class TrajPointRecord {
        private String run_id;
        private String drone_id;
        private Integer seq;
        private Long t_ms;
        private Double x;
        private Double y;
        private Double z;
        private Double vx;
        private Double vy;
        private Double vz;
        private Double ax;
        private Double ay;
        private Double az;
        private String status;
        private String scenario_id;
        private String strategy_id;
        private String strategy_version;
        private String param_set_id;
        private Integer seed;
        private String point_key;

        public String getRun_id() { return run_id; }
        public void setRun_id(String run_id) { this.run_id = run_id; }
        public String getDrone_id() { return drone_id; }
        public void setDrone_id(String drone_id) { this.drone_id = drone_id; }
        public Integer getSeq() { return seq; }
        public void setSeq(Integer seq) { this.seq = seq; }
        public Long getT_ms() { return t_ms; }
        public void setT_ms(Long t_ms) { this.t_ms = t_ms; }
        public Double getX() { return x; }
        public void setX(Double x) { this.x = x; }
        public Double getY() { return y; }
        public void setY(Double y) { this.y = y; }
        public Double getZ() { return z; }
        public void setZ(Double z) { this.z = z; }
        public Double getVx() { return vx; }
        public void setVx(Double vx) { this.vx = vx; }
        public Double getVy() { return vy; }
        public void setVy(Double vy) { this.vy = vy; }
        public Double getVz() { return vz; }
        public void setVz(Double vz) { this.vz = vz; }
        public Double getAx() { return ax; }
        public void setAx(Double ax) { this.ax = ax; }
        public Double getAy() { return ay; }
        public void setAy(Double ay) { this.ay = ay; }
        public Double getAz() { return az; }
        public void setAz(Double az) { this.az = az; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getScenario_id() { return scenario_id; }
        public void setScenario_id(String scenario_id) { this.scenario_id = scenario_id; }
        public String getStrategy_id() { return strategy_id; }
        public void setStrategy_id(String strategy_id) { this.strategy_id = strategy_id; }
        public String getStrategy_version() { return strategy_version; }
        public void setStrategy_version(String strategy_version) { this.strategy_version = strategy_version; }
        public String getParam_set_id() { return param_set_id; }
        public void setParam_set_id(String param_set_id) { this.param_set_id = param_set_id; }
        public Integer getSeed() { return seed; }
        public void setSeed(Integer seed) { this.seed = seed; }
        public String getPoint_key() { return point_key; }
        public void setPoint_key(String point_key) { this.point_key = point_key; }
    }
}

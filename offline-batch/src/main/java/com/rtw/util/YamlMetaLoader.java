package com.rtw.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rtw.model.RunMetaRecord;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统一离线 run_meta 规范化加载器：
 * 1) run_id：优先 YAML，其次父目录名
 * 2) scenario_id：优先 YAML，其次父目录名/文件名派生
 * 3) strategy_id：优先 YAML，其次 transition.method
 * 4) strategy_version：优先 YAML，否则 offline-local
 * 5) param_json：整份 YAML 转 JSON，保证与实时侧 param_json 含义一致（整份参数）
 * 6) param_set_id：sha1(param_json)
 * 7) v_max/a_max：优先 transition.<method>.limits.*，否则全局 limits.*
 * 8) teleport_dist_m：优先 YAML 显式字段，否则外部传入默认值
 */
public class YamlMetaLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    public static RunMetaRecord load(String yamlPath) {
        return load(yamlPath, 10.0d);
    }

    @SuppressWarnings("unchecked")
    public static RunMetaRecord load(String yamlPath, double defaultTeleportDistM) {
        try (FileInputStream fis = new FileInputStream(yamlPath)) {
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(fis);
            if (root == null) {
                throw new IllegalArgumentException("空 YAML: " + yamlPath);
            }

            File yamlFile = new File(yamlPath);
            String parentDirName = yamlFile.getParentFile() != null ? yamlFile.getParentFile().getName() : null;
            String fileBaseName = stripExt(yamlFile.getName());

            RunMetaRecord meta = new RunMetaRecord();

            // 1) run_id
            String runId = asString(root.get("run_id"));
            if (isBlank(runId)) runId = parentDirName;
            if (isBlank(runId)) {
                throw new IllegalArgumentException("无法确定 run_id：YAML 未提供且父目录名为空 -> " + yamlPath);
            }
            meta.setRunId(runId);

            // 2) scenario_id
            String scenarioId = asString(root.get("scenario_id"));
            if (isBlank(scenarioId)) {
                scenarioId = !isBlank(parentDirName) ? parentDirName : ("scn_" + fileBaseName);
            }
            meta.setScenarioId(scenarioId);

            // 3) strategy_id
            String strategyId = asString(root.get("strategy_id"));
            if (isBlank(strategyId)) {
                Map<String, Object> transition = asMap(root.get("transition"));
                strategyId = asString(transition.get("method"));
            }
            if (isBlank(strategyId)) strategyId = "orca";
            meta.setStrategyId(strategyId);

            // 4) strategy_version
            String strategyVersion = asString(root.get("strategy_version"));
            if (isBlank(strategyVersion)) strategyVersion = "offline-local";
            meta.setStrategyVersion(strategyVersion);

            // 5) seed / start / end / status / count / duration
            meta.setSeed(asInt(root.get("seed"), 42));
            meta.setStartMs(asLong(root.get("start_ms"), 0L));
            meta.setEndMs(asLong(root.get("end_ms"), 0L));
            meta.setStatus(defaultIfBlank(asString(root.get("status")), "success"));

            Integer droneCount = extractDroneCount(root);
            meta.setDroneCount(droneCount != null ? droneCount : 0);
            long durationMs = Math.max(0L, meta.getEndMs() - meta.getStartMs());
            meta.setDurationMs(asLong(root.get("duration_ms"), durationMs));

            // 6) param_json / param_set_id：整份 YAML 规范化序列化
            // 使用 LinkedHashMap 尽量保持原字段顺序，提高 param_set_id 稳定性
            String paramJson = MAPPER.writeValueAsString(new LinkedHashMap<>(root));
            meta.setParamJson(paramJson);
            meta.setParamSetId("pset_" + sha1(paramJson).substring(0, 12));

            // 7) 规范化阈值
            double vMax = resolveVxyMax(root, strategyId, 8.0d);
            double aMax = resolveAxyMax(root, strategyId, 2.0d);
            double teleportDistM = resolveTeleportDist(root, defaultTeleportDistM);

            meta.setVMax(vMax);
            meta.setAMax(aMax);
            meta.setTeleportDistM(teleportDistM);

            return meta;
        } catch (Exception e) {
            throw new RuntimeException("加载 YAML 失败: " + yamlPath, e);
        }
    }

    private static Integer extractDroneCount(Map<String, Object> root) {
        Integer direct = asIntObj(root.get("drone_count"));
        if (direct != null) return direct;
        Map<String, Object> ground = asMap(root.get("ground"));
        Integer count = asIntObj(ground.get("count"));
        return count;
    }

    private static double resolveVxyMax(Map<String, Object> root, String strategyId, double fallback) {
        Map<String, Object> strategyNode = asMap(root.get(strategyId));
        Map<String, Object> strategyLimits = asMap(strategyNode.get("limits"));
        Double v1 = asDoubleObj(strategyLimits.get("v_xy_max"));
        if (v1 != null) return v1;

        Map<String, Object> limits = asMap(root.get("limits"));
        Double v2 = asDoubleObj(limits.get("v_xy_max"));
        return v2 != null ? v2 : fallback;
    }

    private static double resolveAxyMax(Map<String, Object> root, String strategyId, double fallback) {
        Map<String, Object> strategyNode = asMap(root.get(strategyId));
        Map<String, Object> strategyLimits = asMap(strategyNode.get("limits"));
        Double a1 = asDoubleObj(strategyLimits.get("a_xy_max"));
        if (a1 != null) return a1;

        Map<String, Object> limits = asMap(root.get("limits"));
        Double a2 = asDoubleObj(limits.get("a_xy_max"));
        return a2 != null ? a2 : fallback;
    }

    private static double resolveTeleportDist(Map<String, Object> root, double fallback) {
        // 优先显式字段，保持后续扩展能力
        Double direct = asDoubleObj(root.get("teleport_dist_m"));
        if (direct != null) return direct;

        // 兼容自定义结构 param.teleport_dist_m
        Map<String, Object> param = asMap(root.get("param"));
        Double nested = asDoubleObj(param.get("teleport_dist_m"));
        if (nested != null) return nested;

        return fallback;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        if (o instanceof Map) return (Map<String, Object>) o;
        return java.util.Collections.emptyMap();
    }

    private static String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static Long asLongObj(Object o) {
        return o instanceof Number ? ((Number) o).longValue() : null;
    }

    private static long asLong(Object o, long def) {
        Long v = asLongObj(o);
        return v != null ? v : def;
    }

    private static Integer asIntObj(Object o) {
        return o instanceof Number ? ((Number) o).intValue() : null;
    }

    private static int asInt(Object o, int def) {
        Integer v = asIntObj(o);
        return v != null ? v : def;
    }

    private static Double asDoubleObj(Object o) {
        return o instanceof Number ? ((Number) o).doubleValue() : null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String defaultIfBlank(String s, String def) {
        return isBlank(s) ? def : s;
    }

    private static String stripExt(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx > 0 ? fileName.substring(0, idx) : fileName;
    }

    private static String sha1(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

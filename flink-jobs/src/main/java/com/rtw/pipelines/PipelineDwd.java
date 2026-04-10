package com.rtw.pipelines;

import com.rtw.config.AppConfig;
import com.rtw.model.*;
import com.rtw.ops.DwdDiffWithMetaFn;
import com.rtw.ops.OverspeedSegmentFn;
import com.rtw.ops.RunQualityAggFn;
import com.rtw.functions.control.ControlSuggestionCooldownFn;
import com.rtw.serde.RunMetaDeser;
import com.rtw.serde.TrajPointDeser;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

/**
 * =========================
 * 实时数仓主链路（当前阶段）
 * =========================
 *
 * 输入（Kafka / ODS）：
 *  1) ods_traj_point  : 轨迹点（高频，40ms 一条）
 *  2) ods_run_meta    : run 配置/结果（低频，start + end）
 *
 * 处理（Flink）：
 *  A) run_meta -> 既写 Doris（可追溯），又 broadcast 给点流（动态阈值）
 *  B) traj_point + broadcast(run_meta params) -> 生成 DWD 明细（速度/加速度/标记）
 *  C) DWD 明细 -> 聚合生成：
 *     - DWS run 级质量汇总（overspeed/overacc/teleport 次数 + TopN drone）
 *     - DWS overspeed 事件段（连续超速段 -> 一条事件）
 *
 * 输出（Doris）：
 *  1) ods_run_meta_v1        : run 事实（UNIQUE(run_id) 覆盖更新）
 *  2) dwd_traj_point_v1      : 点级明细（质量标记）
 *  3) dws_run_quality_v1     : run 级汇总（可增量快照 + 最终定稿）
 *  4) dws_overspeed_event_v1 : 超速事件段（segment）
 */
public class PipelineDwd {

    /** 你现在固定的 run_meta topic（后续可以放到 AppConfig 里） */
    private static final String TOPIC_RUN_META = "ods_run_meta";

    /** 控制建议输出 topic（仿真侧可选消费） */
    private static final String TOPIC_CONTROL_SUGGESTION = "control_suggestion";

    /** Doris 表名（db 通过 cfg.dorisDb 注入） */
    private static final String TBL_RUN_META = "ods_run_meta_v1";
    private static final String TBL_DWS_RUN_QUALITY = "dws_run_quality_v1";
    private static final String TBL_DWS_OVERSPEED_EVENT = "dws_overspeed_event_v1";
    private static final String TBL_DWD_RISK_POINT = "dwd_risk_point_recent";

    public static void build(StreamExecutionEnvironment env, AppConfig cfg) {

        // =========================================================
        // 1) Kafka Source：轨迹点（ODS 点流，高频）
        // =========================================================
        DataStream<OdsTrajPoint> traj = buildTrajPointStream(env, cfg);

        // 同一 topic：分流出 run_end_marker（用于 event-time 定稿），其余走 traj_point
        DataStream<OdsTrajPoint> runEndMarker = traj
                .filter(e -> e != null && "run_end_marker".equals(e.event_type))
                .name("run-end-marker");

        DataStream<OdsTrajPoint> trajPoints = traj
                .filter(e -> e != null && (e.event_type == null || "traj_point".equals(e.event_type)))
                .name("traj-point-only");

        // =========================================================
        // 2) Kafka Source：run_meta（ODS 控制流，低频）
        //    - 同一条 run_meta 流做两件事：
        //      (1) sink Doris（可追溯、便于实验对比）
        //      (2) broadcast 给点流（动态阈值规则计算）
        // =========================================================
        DataStream<OdsRunMeta> runMeta = buildRunMetaStream(env, cfg);

        // 2.1 run_meta 不再写 Doris：仅作为 broadcast 控制流使用
        // sinkRunMetaToDoris(runMeta, cfg);

        // 2.2 run_meta 广播：每个并行实例都能 O(1) 查到 run 的阈值（run_id -> params）
        BroadcastStream<OdsRunMeta> metaBc = runMeta.broadcast(DwdDiffWithMetaFn.RUN_PARAMS_DESC);

        // =========================================================
        // 3) DWD：点流 enrich + 规则计算（动态阈值）
        //    - keyBy(run_id|drone_id) 保证每架无人机在一个 run 内的状态一致
        // =========================================================
        DataStream<DwdTrajPoint> dwd = buildDwdStream(trajPoints, metaBc, cfg);

        // 3.1 不再写全量 DWD 到 Doris
        // sinkDwdToDoris(dwd, cfg);

        // 3.2 仅保留异常点到 Doris（轻量 DWD 排障表）
        DataStream<DwdTrajPoint> riskPoints = dwd
                .filter(p -> p != null &&
                        ((p.is_overspeed != null && p.is_overspeed == 1)
                                || (p.is_overacc != null && p.is_overacc == 1)
                                || (p.is_teleport != null && p.is_teleport == 1)))
                .name("dwd-risk-point-only");

        sinkRiskPointsToDoris(riskPoints, cfg);

        // =========================================================
        // 4) DWS-1：超速事件段（segment）
        //    - “连续超速”合并成一条事件，便于下游统计/回溯
        //    - 事件表 UNIQUE KEY 推荐：dt, run_id, drone_id, start_t_ms
        // =========================================================
        DataStream<DwsOverspeedEvent> osEvents = buildDwsOverspeedEvents(dwd);
        sinkDwsOverspeedEventsToDoris(osEvents, cfg);

        // =========================================================
        // 5) DWS-2：按 run 汇总（overspeed/overacc/teleport 次数 + TopN drone）
        //    - 触发输出依赖 run_end（status != null）
        //    - end 到来后会等一个 grace 窗口吸收尾部点（见 RunQualityAggFn 注释）
        // =========================================================
        DataStream<DwsRunQuality> dwsRunQuality = buildDwsRunQuality(dwd, runEndMarker, cfg);

        // 5.1 DWS 写 Doris
        sinkDwsRunQualityToDoris(dwsRunQuality, cfg);

        // =========================================================
// 6) 碰撞检测 (Collision Detection)
//    - 第一版优化：按 run_id + frame_t_ms 打散同一 run 的不同帧
//    - 仍使用 Tumbling Event-Time Window（40ms）做逐帧快照检测，输出 CollisionHit
// =========================================================

// 6.1 窗口计算：输出 CollisionHit
        DataStream<CollisionHit> collisionHits = dwd
                .filter(p -> p != null && p.run_id != null && !p.run_id.isEmpty())
                // 第一版优化：按 run_id + frame_t_ms 分组
                // 目的：把同一 run 的不同仿真帧打散到多个 subtask 并行执行，降低单 key 热点
                .keyBy(p -> p.run_id + "|" + alignFrame(p.t_ms, cfg.trajFrameMs))
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(
                        java.time.Duration.ofMillis(cfg.trajFrameMs) // 窗口大小 = 帧间隔，例如 40ms
                ))
                // 纯算法逻辑：建 Grid -> 搜 Neighbor -> 输出 Hit
                .process(new com.rtw.functions.collision.CollisionHitWindowFn(cfg.collisionDistM)) // 假设安全距离 3米
                .name("collision-hit-window-by-run-frame");


// =========================================================
// 6.x 控制建议（最小闭环口子）：从 collisionHits 派生 DELAY_DRONE
//     + 200ms 冷却（按 run_id|target）避免连续帧刷爆 topic
// =========================================================
        DataStream<ControlSuggestion> ctrlSuggest = collisionHits
                .map(h -> ControlSuggestion.delayFromCollisionHit(
                        h,
                        2L * cfg.trajFrameMs,   // delay_ms：2 帧
                        2                     // delay_frames
                ))
                .name("ctrl-suggest-from-collision-hit");

        DataStream<ControlSuggestion> ctrlSuggestThrottled = ctrlSuggest
                .keyBy(s -> s.run_id + "|" + (s.targets != null && !s.targets.isEmpty() ? s.targets.get(0) : ""))
                .process(new ControlSuggestionCooldownFn(200))
                .name("ctrl-suggest-cooldown-200ms");

        DataStream<String> ctrlJson = ctrlSuggestThrottled
                .map(new PojoToJsonLine<>())
                .name("ctrl-suggest-to-json");

        ctrlJson.sinkTo(buildKafkaJsonSink(cfg, TOPIC_CONTROL_SUGGESTION))
                .name("sink-kafka-control-suggestion");


// 6.2 聚合为 Segment：输出 DwsCollisionEvent
//    - 需要 run_end_marker 来触发最终定稿 (Event Time Timer)
//    - runEndMarker 流之前已经定义过 (第1步 source 处)
        DataStream<DwsCollisionEvent> collisionEvents = collisionHits
                .keyBy(h -> h.run_id) // 按 run_id 分组，配合 run_end_marker
                .connect(runEndMarker.keyBy(m -> m.run_id))
                .process(new com.rtw.ops.CollisionSegmentAggFn(cfg))
                .name("dws-collision-segment");

// 6.3 Sink 到 Doris
        sinkDwsCollisionEventsToDoris(collisionEvents, cfg);
    }

    // -----------------------------
    // 1) ODS 点流
    // -----------------------------
    private static DataStream<OdsTrajPoint> buildTrajPointStream(StreamExecutionEnvironment env, AppConfig cfg) {
        KafkaSource<OdsTrajPoint> trajSrc = KafkaSource.<OdsTrajPoint>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.topicTrajPoint)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TrajPointDeser())
                .build();

// 事件时间 watermark：生产环境不要假设“严格单调”。Kafka 抖动/重放/多线程写入都可能造成轻微乱序。
        // 这里用 bounded out-of-orderness（建议 1~2 帧）提升鲁棒性。
        // 注意：你上游 run_end_marker 的 t_ms = end_ms + frameMs。
        // 若 watermark 有延迟（maxTs - outOfOrderness），则需要让 marker 的“事件时间”额外推进 outOfOrderness，
        // 否则 end_ms 的 event-time timer 可能迟迟不触发。
        final long ooMs = Math.max(1L, cfg.trajFrameMs * 2L); // 允许最大乱序：2 帧（可按需要调小/调大）
        WatermarkStrategy<OdsTrajPoint> wm = WatermarkStrategy
                .<OdsTrajPoint>forBoundedOutOfOrderness(Duration.ofMillis(ooMs))
                .withTimestampAssigner((e, ts) -> {
                    long base = (e == null) ? 0L : e.t_ms;
                    // 关键：只对 marker “抬高事件时间”用来推进 watermark；不修改记录内容（m.t_ms 仍可按原口径反推 end_ms）。
                    if (e != null && "run_end_marker".equals(e.event_type)) {
                        return base + ooMs;
                    }
                    return base;
                })
                .withIdleness(Duration.ofSeconds(30));  // 关键：让空分区不再卡住 watermark


        return env.fromSource(trajSrc, wm, "kafka-ods-traj-point");
    }

    // -----------------------------
    // 2) ODS run_meta 控制流
    // -----------------------------
    private static DataStream<OdsRunMeta> buildRunMetaStream(StreamExecutionEnvironment env, AppConfig cfg) {
        KafkaSource<OdsRunMeta> metaSrc = KafkaSource.<OdsRunMeta>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(TOPIC_RUN_META)
                .setGroupId(cfg.groupId + "-runmeta")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RunMetaDeser()) // 忽略未知字段，避免字段演进炸作业
                .build();

        // run_meta 是控制流（低频），但它会参与 broadcast join：
        // 两输入算子的 watermark 取 min，如果这里用 noWatermarks，会把下游 event-time 全部“卡死”。
        // 解决：给它一个“很快”的 watermark（用 processing-time 近似），并开启 idleness，避免稀疏流拖慢。
        WatermarkStrategy<OdsRunMeta> wm = WatermarkStrategy
                .<OdsRunMeta>forMonotonousTimestamps()
                .withTimestampAssigner((e, ts) -> System.currentTimeMillis())
                .withIdleness(Duration.ofSeconds(30));

        return env.fromSource(metaSrc, wm, "kafka-ods-run-meta");
    }

    // -----------------------------
    // 3) DWD：点流 enrich + 规则计算
    // -----------------------------
    private static DataStream<DwdTrajPoint> buildDwdStream(
            DataStream<OdsTrajPoint> traj,
            BroadcastStream<OdsRunMeta> metaBc,
            AppConfig cfg
    ) {
        KeyedStream<OdsTrajPoint, String> keyedTraj = traj.keyBy(OdsTrajPoint::keyRunDrone);

        // 关键：KeyedBroadcastProcessFunction
        // - 点流：keyed，内部维护 last build 差分状态
        // - run_meta：broadcast，维护 run_id -> params
        return keyedTraj
                .connect(metaBc)
                .process(new DwdDiffWithMetaFn(cfg))
                .name("dwd-diff-with-meta");
    }

    // -----------------------------
    // 4) DWS：overspeed segment（按 run_id|drone_id）
    // -----------------------------
    private static DataStream<DwsOverspeedEvent> buildDwsOverspeedEvents(DataStream<DwdTrajPoint> dwd) {
        return dwd
                // overspeed segment 必须按无人机保序（同一 run 同一 drone）
                .keyBy(p -> p.run_id + "|" + p.drone_id)
                .process(new OverspeedSegmentFn())
                .name("dws-overspeed-segment");
    }

    // -----------------------------
    // 5) DWS：按 run 聚合（run_end 触发输出）
    // -----------------------------
    private static long alignFrame(long tMs, long frameMs) {
        return Math.floorDiv(tMs, frameMs) * frameMs;
    }

    private static DataStream<DwsRunQuality> buildDwsRunQuality(
            DataStream<DwdTrajPoint> dwd,
            DataStream<OdsTrajPoint> runEndMarker,
            AppConfig cfg
    ) {
        // run_end_marker：来自同一条点流（同 topic 分流），用 event_type=run_end_marker
        // 关键点：RunQualityAggFn 使用 event-time timer(end_ms) 定稿
        DataStream<OdsTrajPoint> runEnd = runEndMarker
                .filter(m -> m != null && m.run_id != null && !m.run_id.isEmpty())
                .name("run-end-marker-only");

        return dwd
                .keyBy(p -> p.run_id)
                .connect(runEnd.keyBy(m -> m.run_id))
                .process(new RunQualityAggFn(cfg))
                .name("dws-run-quality");
    }
    // =========================================================
// Kafka Sink（最小闭环口子）：写 JSON 行到指定 topic
// =========================================================
    private static KafkaSink<String> buildKafkaJsonSink(AppConfig cfg, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 最小实现：允许重复，依靠 suggestion_id 幂等（或下游去重）
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }



    // =========================================================
    // Doris Sink（通用工具）：写 JSON 行（每行一个对象）
    // =========================================================
    private static DorisSink<String> buildDorisJsonLineSink(AppConfig cfg, String tableIdentifier, String labelPrefix) {
        String fenodes = cfg.dorisFeHost + ":" + cfg.dorisFeHttpPort;

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(fenodes)
                .setTableIdentifier(tableIdentifier)
                .setUsername(cfg.dorisUser)
                .setPassword(cfg.dorisPassword)
                .build();

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        DorisExecutionOptions exec = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)  // 便于排查导入（Doris 侧按 label 查）
                .setDeletable(false)
                .setStreamLoadProp(props)
                .build();

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(exec)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions);

        return builder.build();
    }

    // -----------------------------
    // run_meta 写 Doris（可追溯 + UNIQUE 覆盖更新）
    // -----------------------------
    private static void sinkRunMetaToDoris(DataStream<OdsRunMeta> runMeta, AppConfig cfg) {
        // 只输出 Doris 表里存在的列，避免 producer 多字段导致导入失败
        DataStream<String> jsonLines = runMeta
                .map(new RunMetaToJsonLine())
                .name("runmeta-to-json-line");

        String tableId = cfg.dorisDb + "." + TBL_RUN_META;
        jsonLines.sinkTo(buildDorisJsonLineSink(cfg, tableId, "rtw_runmeta"))
                .name("sink-doris-runmeta");
    }



    // -----------------------------
    // 仅异常点写 Doris（轻量 DWD 排障表）
    // -----------------------------
    private static void sinkRiskPointsToDoris(DataStream<DwdTrajPoint> riskPoints, AppConfig cfg) {
        DataStream<String> jsonLines = riskPoints
                .map(new RiskPointToJsonLine())
                .name("risk-point-to-json-line");

        String tableId = cfg.dorisDb + "." + TBL_DWD_RISK_POINT;
        jsonLines.sinkTo(buildDorisJsonLineSink(cfg, tableId, "rtw_dwd_risk"))
                .name("sink-doris-dwd-risk-point");
    }

    // -----------------------------
    // DWS run_quality 写 Doris
    // -----------------------------
    private static void sinkDwsRunQualityToDoris(DataStream<DwsRunQuality> dws, AppConfig cfg) {
        DataStream<String> jsonLines = dws
                .map(new PojoToJsonLine<>())
                .name("dws-runq-to-json-line");

        String tableId = cfg.dorisDb + "." + TBL_DWS_RUN_QUALITY;
        jsonLines.sinkTo(buildDorisJsonLineSink(cfg, tableId, "rtw_dws_runq"))
                .name("sink-doris-dws-run-quality");
    }

    // -----------------------------
    // DWS overspeed_event 写 Doris
    // -----------------------------
    private static void sinkDwsOverspeedEventsToDoris(DataStream<DwsOverspeedEvent> events, AppConfig cfg) {
        DataStream<String> jsonLines = events
                .map(new PojoToJsonLine<>())
                .name("dws-os-to-json-line");

        String tableId = cfg.dorisDb + "." + TBL_DWS_OVERSPEED_EVENT;
        jsonLines.sinkTo(buildDorisJsonLineSink(cfg, tableId, "rtw_dws_os"))
                .name("sink-doris-dws-overspeed-event");
    }

    /** 通用：POJO -> json line（用 Flink shaded jackson，避免版本冲突） */
    private static class PojoToJsonLine<T> extends RichMapFunction<T, String> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(T value) throws Exception {
            return mapper.writeValueAsString(value);
        }
    }

    /** run_meta：只输出表里存在的列（UNIQUE 覆盖更新靠 run_id） */
    private static class RunMetaToJsonLine extends RichMapFunction<OdsRunMeta, String> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(OdsRunMeta m) throws Exception {
            ObjectNode n = mapper.createObjectNode();

            // UNIQUE KEY
            n.put("run_id", m.run_id);

            // 基础字段
            n.put("event_type", m.event_type != null ? m.event_type : "run_meta");
            if (m.start_ms != null) n.put("start_ms", m.start_ms); else n.putNull("start_ms");

            if (m.schema_version != null) n.put("schema_version", m.schema_version); else n.putNull("schema_version");
            if (m.scenario_id != null) n.put("scenario_id", m.scenario_id); else n.putNull("scenario_id");

            // end/update 字段（end 消息会补齐）
            if (m.end_ms != null) n.put("end_ms", m.end_ms); else n.putNull("end_ms");
            if (m.duration_ms != null) n.put("duration_ms", m.duration_ms); else n.putNull("duration_ms");
            if (m.status != null) n.put("status", m.status); else n.putNull("status");

            // 实验维度
            if (m.strategy_id != null) n.put("strategy_id", m.strategy_id); else n.putNull("strategy_id");
            if (m.strategy_version != null) n.put("strategy_version", m.strategy_version); else n.putNull("strategy_version");
            if (m.param_set_id != null) n.put("param_set_id", m.param_set_id); else n.putNull("param_set_id");
            if (m.seed != null) n.put("seed", m.seed); else n.putNull("seed");
            if (m.param_json != null) n.put("param_json", m.param_json); else n.putNull("param_json");



            return mapper.writeValueAsString(n);
        }
    }


    /** 轻量异常点：仅输出 dwd_risk_point_recent 表里需要的列 */
    private static class RiskPointToJsonLine extends RichMapFunction<DwdTrajPoint, String> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(DwdTrajPoint d) throws Exception {
            ObjectNode n = mapper.createObjectNode();

            // UNIQUE KEY
            n.put("dt", d.dt);
            n.put("run_id", d.run_id);
            n.put("drone_id", d.drone_id);
            n.put("seq", d.seq);

            // 派生点主键，便于排障/对账
            n.put("point_key", d.run_id + "|" + d.drone_id + "|" + d.seq);

            // 基础字段
            if (d.scenario_id != null) n.put("scenario_id", d.scenario_id); else n.putNull("scenario_id");
            n.put("t_ms", d.t_ms);
            n.put("x", d.x);
            n.put("y", d.y);
            n.put("z", d.z);

            // 风险指标
            n.put("speed", d.speed != null ? d.speed : null);
            n.put("acc", d.acc != null ? d.acc : null);

            if (d.is_time_back != null) n.put("is_time_back", d.is_time_back); else n.putNull("is_time_back");
            if (d.is_teleport != null) n.put("is_teleport", d.is_teleport); else n.putNull("is_teleport");
            if (d.is_overspeed != null) n.put("is_overspeed", d.is_overspeed); else n.putNull("is_overspeed");
            if (d.is_overacc != null) n.put("is_overacc", d.is_overacc); else n.putNull("is_overacc");
            if (d.is_stuck != null) n.put("is_stuck", d.is_stuck); else n.putNull("is_stuck");

            // 维度字段
            if (d.strategy_id != null) n.put("strategy_id", d.strategy_id); else n.putNull("strategy_id");
            if (d.strategy_version != null) n.put("strategy_version", d.strategy_version); else n.putNull("strategy_version");
            if (d.param_set_id != null) n.put("param_set_id", d.param_set_id); else n.putNull("param_set_id");
            if (d.seed != null) n.put("seed", d.seed); else n.putNull("seed");

            // 更新时间
            n.put("update_ms", System.currentTimeMillis());

            return mapper.writeValueAsString(n);
        }
    }
    private static void sinkDwsCollisionEventsToDoris(DataStream<DwsCollisionEvent> events, AppConfig cfg) {
        // 假设你在 AppConfig 里加了 dwsTableCollision
        String tableId = cfg.dorisDb + ".dws_collision_event_v1";

        DataStream<String> jsonLines = events
                .map(new PojoToJsonLine<>()) // 复用之前的泛型 Map
                .name("dws-collision-to-json");

        jsonLines.sinkTo(buildDorisJsonLineSink(cfg, tableId, "rtw_dws_coll"))
                .name("sink-doris-dws-collision");
    }
}

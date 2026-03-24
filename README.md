# Flink 实时作业（rt-warehouse-uav）

本模块构建一条实时 Flink 流式链路：从 Kafka 读取无人机轨迹与 run 元数据，做 DWD 明细指标与规则标记，并在 DWS 层聚合多类结果，最终写入 Doris。

## 功能概览
- 输入（Kafka）
  - `ods_traj_point`：高频轨迹点
  - `ods_run_meta`：低频 run 元信息（start/end/params）
- 核心处理
  - DWD 规则计算与标记（超速/过加速度/瞬移/时间回退）
  - DWS 聚合：
    - 超速事件段
    - run 质量汇总（Top-N 无人机评分）
    - 碰撞检测（窗口命中 + 段合并）
- 输出（Doris）
  - `ods_run_meta_v1`
  - `dwd_traj_point_v1`
  - `dws_overspeed_event_v1`
  - `dws_run_quality_v1`
  - `dws_collision_event_v1`

## 关键结构
- `src/main/java/com/rtw/Main.java` 入口
- `src/main/java/com/rtw/pipelines/PipelineDwd.java` 任务编排
- `src/main/java/com/rtw/config/AppConfig.java` 配置读取
- `src/main/java/com/rtw/ops` 主要算子实现

## 环境依赖
- JDK 11
- Maven 3.8+
- Apache Flink 1.20.x
- Kafka（已创建 Topic，且有数据）
- Apache Doris（已建表，开启 Stream Load）

## 构建
```bash
mvn -q -DskipTests package
```
产物：
- `target/flink-app-1.0-SNAPSHOT.jar`
- `target/flink-app-1.0-SNAPSHOT-shaded.jar`

## 运行参数
全部参数支持环境变量或 `--key=value` 形式传入。

常用/必需：
- `KAFKA_BOOTSTRAP`（例：`localhost:9092`）
- `KAFKA_TOPIC_TRAJ_POINT`（默认：`ods_traj_point`）
- `KAFKA_GROUP_ID`（默认：`rtw-dwd-v1`）

- `DORIS_FE_HOST`（例：`localhost`）
- `DORIS_FE_HTTP_PORT`（默认：`8030`）
- `DORIS_USER`
- `DORIS_PASSWORD`
- `DORIS_DB`（默认：`uav_dw`）
- `DORIS_TABLE_DWD`（默认：`dwd_traj_point_v1`）

Flink / 规则相关：
- `FLINK_PARALLELISM`（默认：`1`）
- `WATERMARK_OOO_MS`（默认：`200`）
- `TRAJ_FRAME_MS`（默认：`40`）
- `TELEPORT_DIST_M`（默认：`20.0`)
- `V_MAX`（默认：`8.0`）
- `A_MAX`（默认：`6.0`）
- `COLLISION_DIST_M`（默认：`3.0`）

DWS 输出：
- `DWS_EMIT_MODE`（`FINAL_ONLY` 或 `INCR_AND_FINAL`）
- `DWS_EMIT_INTERVAL_MS`（默认：`10000`）
- `DWS_FINAL_GRACE_MS`（默认：`5000`）
- `DWS_TOP_N`（默认：`5`）
- `SCORE_W_OS`（默认：`2`）
- `SCORE_W_OA`（默认：`3`）
- `SCORE_W_TP`（默认：`10`）

## 输入样例（Kafka）
### ods_traj_point
```json
{
  "schema_version": "v1",
  "event_type": "traj_point",
  "scenario_id": "scn_001",
  "run_id": "run_001",
  "drone_id": "drone_01",
  "t_ms": 1700000000000,
  "seq": 1,
  "x": 0.1, "y": 0.2, "z": 5.0,
  "strategy_id": "stg_a",
  "strategy_version": "1",
  "param_set_id": "p1",
  "seed": 42
}
```

### ods_run_meta
```json
{
  "schema_version": "v1",
  "event_type": "run_meta",
  "scenario_id": "scn_001",
  "run_id": "run_001",
  "start_ms": 1700000000000,
  "strategy_id": "stg_a",
  "strategy_version": "1",
  "param_set_id": "p1",
  "seed": 42,
  "param_json": "{\"v_max\":8.0,\"a_max\":6.0,\"teleport_dist_m\":20.0}"
}
```

### run_end_marker（与 traj 点同 topic）
```json
{
  "event_type": "run_end_marker",
  "run_id": "run_001",
  "t_ms": 1700000005000,
  "status": "success",
  "drone_count": 10
}
```

## 本地运行（带 Web UI）
使用包含 Flink 运行时依赖的 Maven profile：
```bash
mvn -q -DskipTests -Plocal-run exec:java \
  -Dexec.mainClass="com.rtw.Main" \
  -Dexec.args="--KAFKA_BOOTSTRAP=localhost:9092 --DORIS_FE_HOST=localhost --DORIS_USER=flink --DORIS_PASSWORD=***"
```

## 提交到 Flink 集群
```bash
flink run -c com.rtw.Main target/flink-app-1.0-SNAPSHOT-shaded.jar \
  --KAFKA_BOOTSTRAP=localhost:9092 \
  --DORIS_FE_HOST=localhost \
  --DORIS_USER=flink \
  --DORIS_PASSWORD=***
```

## Doris 表要求
需要在 `DORIS_DB` 中创建以下表：
- `ods_run_meta_v1`
- `dwd_traj_point_v1`
- `dws_run_quality_v1`
- `dws_overspeed_event_v1`
- `dws_collision_event_v1`

列定义需与以下 POJO 对齐：
- `src/main/java/com/rtw/model`

## 备注 / 已知限制
- `Main` 目前默认使用本地环境，集群场景需自行调整。
- 状态清理依赖 run_end_marker；长期运行可考虑增加 TTL。
- 轨迹点水位假设严格单调，乱序场景需调整策略。

## 快速检查
- Kafka topic 已创建且有数据
- Doris FE 可访问且 Stream Load 可用
- 环境变量已配置
- JAR 构建成功

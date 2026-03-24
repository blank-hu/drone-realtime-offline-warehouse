package com.rtw.job;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.HiveTableManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

/**
 * DWS 构建：连续超速事件段（overspeed segment）
 *
 * 对齐实时 OverspeedSegmentFn 的核心语义：
 * 1) 连续 is_overspeed=1 的点合并成一条事件
 * 2) 允许最多 1 个非超速点夹在段内（等价于相邻超速点 seq gap <= 2）
 * 3) 少于 3 个超速点的段视为噪声，不输出
 * 4) 事件 ID 采用：drone_id|overspeed|start_seq
 */
public class DwsOverspeedEventBuildJob {

    private static final int MIN_POINTS = 3;
    private static final int CLOSE_AFTER_NON_OS = 2;
    private static final int MAX_SEQ_GAP = 20;

    public void run(SparkSession spark) {
        HiveTableManager.initDatabase(spark);
        spark.conf().set("spark.sql.session.timeZone", "UTC");

        Dataset<Row> dwdPoint = spark.table(OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di");
        dwdPoint.createOrReplaceTempView("tmp_dwd_traj_point_detail_di");

        String sql =
                "with ordered as ( " +
                        "  select " +
                        "    run_id, drone_id, seq, t_ms, x, y, z, speed, " +
                        "    scenario_id, strategy_id, strategy_version, param_set_id, seed, " +
                        "    cast(coalesce(is_overspeed, 0) as int) as is_overspeed, " +
                        "    coalesce(seq - lag(seq) over(partition by run_id, drone_id order by seq), 1) as seq_gap " +
                        "  from tmp_dwd_traj_point_detail_di " +
                        "), chunked as ( " +
                        "  select *, " +
                        "    sum(case when seq_gap > " + MAX_SEQ_GAP + " then 1 else 0 end) " +
                        "      over(partition by run_id, drone_id order by seq rows between unbounded preceding and current row) as chunk_id " +
                        "  from ordered " +
                        "), os_only as ( " +
                        "  select *, " +
                        "    lag(seq) over(partition by run_id, drone_id, chunk_id order by seq) as prev_os_seq " +
                        "  from chunked " +
                        "  where is_overspeed = 1 " +
                        "), seg_mark as ( " +
                        "  select *, " +
                        "    case " +
                        "      when prev_os_seq is null then 1 " +
                        "      when seq - prev_os_seq > " + CLOSE_AFTER_NON_OS + " then 1 " +
                        "      else 0 " +
                        "    end as is_new_seg " +
                        "  from os_only " +
                        "), seg_ided as ( " +
                        "  select *, " +
                        "    sum(is_new_seg) over(partition by run_id, drone_id, chunk_id order by seq rows between unbounded preceding and current row) as seg_id " +
                        "  from seg_mark " +
                        "), seg_agg as ( " +
                        "  select " +
                        "    run_id, drone_id, chunk_id, seg_id, " +
                        "    min(seq) as start_seq, " +
                        "    max(seq) as end_seq, " +
                        "    min(t_ms) as start_t_ms, " +
                        "    max(t_ms) as end_t_ms, " +
                        "    cast(count(1) as int) as points_cnt, " +
                        "    max(speed) as max_speed, " +
                        "    avg(speed) as avg_speed, " +
                        "    max(scenario_id) as scenario_id, " +
                        "    max(strategy_id) as strategy_id, " +
                        "    max(strategy_version) as strategy_version, " +
                        "    max(param_set_id) as param_set_id, " +
                        "    max(seed) as seed " +
                        "  from seg_ided " +
                        "  group by run_id, drone_id, chunk_id, seg_id " +
                        "  having count(1) >= " + MIN_POINTS + " " +
                        "), start_pt as ( " +
                        "  select run_id, drone_id, chunk_id, seg_id, seq as start_seq, x as start_x, y as start_y, z as start_z " +
                        "  from seg_ided " +
                        "), end_pt as ( " +
                        "  select run_id, drone_id, chunk_id, seg_id, seq as end_seq, x as end_x, y as end_y, z as end_z " +
                        "  from seg_ided " +
                        ") " +
                        "select " +
                        "  to_date(from_unixtime(cast(a.start_t_ms / 1000 as bigint))) as dt, " +
                        "  a.run_id, " +
                        "  a.scenario_id, " +
                        "  a.drone_id, " +
                        "  concat(a.drone_id, '|overspeed|', cast(a.start_seq as string)) as event_id, " +
                        "  a.start_seq, " +
                        "  a.end_seq, " +
                        "  a.start_t_ms, " +
                        "  a.end_t_ms, " +
                        "  greatest(cast(0 as bigint), a.end_t_ms - a.start_t_ms) as duration_ms, " +
                        "  a.points_cnt, " +
                        "  a.max_speed, " +
                        "  a.avg_speed, " +
                        "  s.start_x, s.start_y, s.start_z, " +
                        "  e.end_x, e.end_y, e.end_z, " +
                        "  a.strategy_id, " +
                        "  a.strategy_version, " +
                        "  a.param_set_id, " +
                        "  a.seed " +
                        "from seg_agg a " +
                        "join start_pt s " +
                        "  on a.run_id = s.run_id and a.drone_id = s.drone_id and a.chunk_id = s.chunk_id and a.seg_id = s.seg_id and a.start_seq = s.start_seq " +
                        "join end_pt e " +
                        "  on a.run_id = e.run_id and a.drone_id = e.drone_id and a.chunk_id = e.chunk_id and a.seg_id = e.seg_id and a.end_seq = e.end_seq";

        Dataset<Row> result = spark.sql(sql)
                .withColumn("update_ms", lit(System.currentTimeMillis()));

        System.out.println("===== dws_overspeed_event_di schema =====");
        result.printSchema();
        result.orderBy("run_id", "drone_id", "start_seq").show(100, false);

        result.write()
                .mode(SaveMode.Overwrite)
                .parquet(OfflineJobConfig.DWS_OVERSPEED_EVENT_PATH);

        HiveTableManager.createDwsOverspeedEventTable(spark);

        spark.sql("select * from " + OfflineJobConfig.DB_NAME + ".dws_overspeed_event_di order by run_id, drone_id, start_seq limit 100")
                .show(false);
    }
}

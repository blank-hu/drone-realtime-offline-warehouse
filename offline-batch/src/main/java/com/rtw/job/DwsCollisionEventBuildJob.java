package com.rtw.job;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.HiveTableManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

/**
 * DWS 构建：collision event（碰撞事件段）
 *
 * 语义对齐实时链路：
 * 1) 同一帧(frame_t_ms)内，每个 drone 只保留 t_ms 最大的一条点
 * 2) 同帧 pair 仅保留 drone_a < drone_b，避免 A-B / B-A 重复
 * 3) dist <= d_min 记为一次 collision hit
 * 4) 同一 pair 相邻 hit 若 frame_t_ms 间隔 <= 2 * frameMs，则合并为同一段
 * 5) end_t_ms = 最后一帧命中的 frame_t_ms + frameMs
 * 6) event_id = drone_a|drone_b|collision|start_t_ms
 *
 * 当前离线实现为了稳定可跑，采用“同帧 self join + bbox 预过滤”方式，
 * 先保证语义与实时一致；后续数据量上来，再替换为空间网格版 SQL。
 */
public class DwsCollisionEventBuildJob {

    public void run(SparkSession spark) {
        HiveTableManager.initDatabase(spark);

        // 与实时 dtUtc(start_t_ms) 语义对齐，统一按 UTC 取日期
        spark.conf().set("spark.sql.session.timeZone", "UTC");

        Dataset<Row> dwdPoint = spark.table(OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di");
        dwdPoint.createOrReplaceTempView("tmp_dwd_traj_point_detail_di");

        final long frameMs = OfflineJobConfig.TRAJ_FRAME_MS;
        final long maxGapMs = Math.max(frameMs, frameMs * 2L);
        final double dMin = OfflineJobConfig.COLLISION_DIST_M;
        final long updateMs = System.currentTimeMillis();

        String sql =
                "with frame_base as ( " +
                        "  select " +
                        "    run_id, drone_id, seq, t_ms, x, y, z, " +
                        "    strategy_id, strategy_version, param_set_id, seed, " +
                        "    cast(floor(t_ms / " + ((double) frameMs) + ") * " + frameMs + " as bigint) as frame_t_ms, " +
                        "    row_number() over ( " +
                        "      partition by run_id, cast(floor(t_ms / " + ((double) frameMs) + ") * " + frameMs + " as bigint), drone_id " +
                        "      order by t_ms desc, seq desc " +
                        "    ) as rn " +
                        "  from tmp_dwd_traj_point_detail_di " +
                        "), frame_latest as ( " +
                        "  select " +
                        "    run_id, drone_id, seq, t_ms, x, y, z, " +
                        "    strategy_id, strategy_version, param_set_id, seed, frame_t_ms " +
                        "  from frame_base " +
                        "  where rn = 1 " +
                        "), hit_raw as ( " +
                        "  select " +
                        "    a.run_id, " +
                        "    a.frame_t_ms, " +
                        "    a.drone_id as drone_a, " +
                        "    b.drone_id as drone_b, " +
                        "    sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2) + pow(a.z - b.z, 2)) as dist, " +
                        "    cast(" + dMin + " as double) as d_min, " +
                        "    a.x as ax, a.y as ay, a.z as az, " +
                        "    b.x as bx, b.y as by, b.z as bz, " +
                        "    a.strategy_id as strategy_id, " +
                        "    a.strategy_version as strategy_version, " +
                        "    a.param_set_id as param_set_id, " +
                        "    a.seed as seed " +
                        "  from frame_latest a " +
                        "  join frame_latest b " +
                        "    on a.run_id = b.run_id " +
                        "   and a.frame_t_ms = b.frame_t_ms " +
                        "   and a.drone_id < b.drone_id " +
                        "   and abs(a.x - b.x) <= " + dMin + " " +
                        "   and abs(a.y - b.y) <= " + dMin + " " +
                        "   and abs(a.z - b.z) <= " + dMin + " " +
                        "), hit_filtered as ( " +
                        "  select * " +
                        "  from hit_raw " +
                        "  where dist <= d_min " +
                        "), seg_mark as ( " +
                        "  select *, " +
                        "    lag(frame_t_ms) over(partition by run_id, drone_a, drone_b order by frame_t_ms) as prev_frame_t_ms " +
                        "  from hit_filtered " +
                        "), seg_flag as ( " +
                        "  select *, " +
                        "    case " +
                        "      when prev_frame_t_ms is null then 1 " +
                        "      when frame_t_ms - prev_frame_t_ms > " + maxGapMs + " then 1 " +
                        "      else 0 " +
                        "    end as is_new_seg " +
                        "  from seg_mark " +
                        "), seg_ided as ( " +
                        "  select *, " +
                        "    sum(is_new_seg) over ( " +
                        "      partition by run_id, drone_a, drone_b " +
                        "      order by frame_t_ms " +
                        "      rows between unbounded preceding and current row " +
                        "    ) as seg_id " +
                        "  from seg_flag " +
                        "), seg_ranked as ( " +
                        "  select *, " +
                        "    row_number() over(partition by run_id, drone_a, drone_b, seg_id order by frame_t_ms asc) as seg_rk_asc, " +
                        "    row_number() over(partition by run_id, drone_a, drone_b, seg_id order by frame_t_ms desc) as seg_rk_desc " +
                        "  from seg_ided " +
                        "), seg_agg as ( " +
                        "  select " +
                        "    run_id, drone_a, drone_b, seg_id, " +
                        "    min(frame_t_ms) as start_t_ms, " +
                        "    max(frame_t_ms) + " + frameMs + " as end_t_ms, " +
                        "    cast(count(1) as int) as frames_cnt, " +
                        "    min(dist) as min_dist, " +
                        "    avg(dist) as avg_dist, " +
                        "    max(d_min) as d_min, " +
                        "    max(strategy_id) as strategy_id, " +
                        "    max(strategy_version) as strategy_version, " +
                        "    max(param_set_id) as param_set_id, " +
                        "    max(seed) as seed " +
                        "  from seg_ranked " +
                        "  group by run_id, drone_a, drone_b, seg_id " +
                        "), start_hit as ( " +
                        "  select " +
                        "    run_id, drone_a, drone_b, seg_id, " +
                        "    ax as start_ax, ay as start_ay, az as start_az, " +
                        "    bx as start_bx, by as start_by, bz as start_bz " +
                        "  from seg_ranked " +
                        "  where seg_rk_asc = 1 " +
                        "), end_hit as ( " +
                        "  select " +
                        "    run_id, drone_a, drone_b, seg_id, " +
                        "    ax as end_ax, ay as end_ay, az as end_az, " +
                        "    bx as end_bx, by as end_by, bz as end_bz " +
                        "  from seg_ranked " +
                        "  where seg_rk_desc = 1 " +
                        ") " +
                        "select " +
                        "  case " +
                        "    when a.start_t_ms >= 1000000000000 then to_date(from_unixtime(cast(a.start_t_ms / 1000 as bigint))) " +
                        "    else null " +
                        "  end as dt, " +
                        "  a.run_id, " +
                        "  a.drone_a, " +
                        "  a.drone_b, " +
                        "  a.start_t_ms, " +
                        "  concat(a.drone_a, '|', a.drone_b, '|collision|', cast(a.start_t_ms as string)) as event_id, " +
                        "  a.end_t_ms, " +
                        "  greatest(cast(0 as bigint), a.end_t_ms - a.start_t_ms) as duration_ms, " +
                        "  a.frames_cnt, " +
                        "  a.min_dist, " +
                        "  a.avg_dist, " +
                        "  a.d_min, " +
                        "  s.start_ax, s.start_ay, s.start_az, " +
                        "  s.start_bx, s.start_by, s.start_bz, " +
                        "  e.end_ax, e.end_ay, e.end_az, " +
                        "  e.end_bx, e.end_by, e.end_bz, " +
                        "  a.strategy_id, " +
                        "  a.strategy_version, " +
                        "  a.param_set_id, " +
                        "  a.seed " +
                        "from seg_agg a " +
                        "join start_hit s " +
                        "  on a.run_id = s.run_id " +
                        " and a.drone_a = s.drone_a " +
                        " and a.drone_b = s.drone_b " +
                        " and a.seg_id = s.seg_id " +
                        "join end_hit e " +
                        "  on a.run_id = e.run_id " +
                        " and a.drone_a = e.drone_a " +
                        " and a.drone_b = e.drone_b " +
                        " and a.seg_id = e.seg_id";

        Dataset<Row> result = spark.sql(sql)
                .withColumn("update_ms", lit(updateMs));

        System.out.println("===== dws_collision_event_di schema =====");
        result.printSchema();
        result.orderBy("run_id", "drone_a", "drone_b", "start_t_ms").show(100, false);

        result.write()
                .mode(SaveMode.Overwrite)
                .parquet(OfflineJobConfig.DWS_COLLISION_EVENT_PATH);

        HiveTableManager.createDwsCollisionEventTable(spark);

        spark.sql("select * from " + OfflineJobConfig.DB_NAME +
                        ".dws_collision_event_di order by run_id, drone_a, drone_b, start_t_ms limit 100")
                .show(false);
    }
}
package com.rtw.job;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.HiveTableManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * ADS 构建：统一控制评估表
 *
 * 评估维度：
 * 1) 碰撞风险：碰撞段次数、持续帧数、最小距离、平均距离
 * 2) 场景维度：scenario_id + strategy_id + strategy_version + param_set_id
 * 3) 网格热点：逐帧近风险对落到空间网格，统计热点数量与集中度
 * 4) 运动平稳性：overspeed / overacc / teleport + speed/acc 分位与波动度
 *
 * 输出：ads_control_eval_di
 */
public class AdsControlEvalBuildJob {

    public void run(SparkSession spark) {
        HiveTableManager.initDatabase(spark);
        spark.conf().set("spark.sql.session.timeZone", "UTC");

        final long frameMs = OfflineJobConfig.TRAJ_FRAME_MS;
        final double collisionDist = OfflineJobConfig.COLLISION_DIST_M;
        final double hotspotNearDist = OfflineJobConfig.HOTSPOT_NEAR_DIST_M;
        final double hotspotCellSize = OfflineJobConfig.HOTSPOT_CELL_SIZE_M;

        final double wCollisionEvent = OfflineJobConfig.EVAL_W_COLLISION_EVENT;
        final double wCollisionFrame = OfflineJobConfig.EVAL_W_COLLISION_FRAME;
        final double wCollisionDistGap = OfflineJobConfig.EVAL_W_COLLISION_DIST_GAP;
        final double wHotspotHit = OfflineJobConfig.EVAL_W_HOTSPOT_HIT;
        final double wHotspotCell = OfflineJobConfig.EVAL_W_HOTSPOT_CELL;
        final double wHotspotTop1Ratio = OfflineJobConfig.EVAL_W_HOTSPOT_TOP1_RATIO;
        final double wOverspeed = OfflineJobConfig.EVAL_W_OVERSPEED;
        final double wOveracc = OfflineJobConfig.EVAL_W_OVERACC;
        final double wTeleport = OfflineJobConfig.EVAL_W_TELEPORT;
        final double wSpeedP95 = OfflineJobConfig.EVAL_W_SPEED_P95;
        final double wAbsAccP95 = OfflineJobConfig.EVAL_W_ABS_ACC_P95;

        final double levelAMax = OfflineJobConfig.EVAL_LEVEL_A_MAX;
        final double levelBMax = OfflineJobConfig.EVAL_LEVEL_B_MAX;
        final double levelCMax = OfflineJobConfig.EVAL_LEVEL_C_MAX;

        String sql =
                "with run_dim as ( " +
                        "    select " +
                        "        q.dt, " +
                        "        q.run_id, " +
                        "        q.scenario_id, " +
                        "        q.strategy_id, " +
                        "        q.strategy_version, " +
                        "        q.param_set_id, " +
                        "        q.drone_cnt, " +
                        "        q.duration_ms, " +
                        "        q.status, " +
                        "        q.overspeed_cnt, " +
                        "        q.overacc_cnt, " +
                        "        q.teleport_cnt, " +
                        "        m.param_json, " +
                        "        m.v_max as base_v_max, " +
                        "        m.a_max as base_a_max, " +
                        "        m.teleport_dist_m as base_teleport_dist_m " +
                        "    from " + OfflineJobConfig.DB_NAME + ".dws_run_quality_di q " +
                        "    left join " + OfflineJobConfig.DB_NAME + ".ods_run_meta_di m " +
                        "        on q.run_id = m.run_id " +
                        "), " +
                        "collision_run as ( " +
                        "    select " +
                        "        run_id, " +
                        "        count(*) as collision_event_cnt, " +
                        "        sum(frames_cnt) as collision_frames_cnt, " +
                        "        min(min_dist) as collision_min_dist, " +
                        "        avg(avg_dist) as collision_avg_dist, " +
                        "        avg(duration_ms) as collision_avg_duration_ms " +
                        "    from " + OfflineJobConfig.DB_NAME + ".dws_collision_event_di " +
                        "    group by run_id " +
                        "), " +
                        "motion_run as ( " +
                        "    select " +
                        "        run_id, " +
                        "        percentile_approx(speed, 0.95, 1000) as speed_p95, " +
                        "        percentile_approx(abs(acc), 0.95, 1000) as abs_acc_p95, " +
                        "        stddev_pop(speed) as speed_std, " +
                        "        stddev_pop(abs(acc)) as abs_acc_std " +
                        "    from " + OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di " +
                        "    group by run_id " +
                        "), " +
                        "frame_base as ( " +
                        "    select " +
                        "        run_id, " +
                        "        drone_id, " +
                        "        seq, " +
                        "        t_ms, " +
                        "        x, " +
                        "        y, " +
                        "        z, " +
                        "        cast(floor(t_ms / " + ((double) frameMs) + ") * " + frameMs + " as bigint) as frame_t_ms, " +
                        "        row_number() over ( " +
                        "            partition by run_id, cast(floor(t_ms / " + ((double) frameMs) + ") * " + frameMs + " as bigint), drone_id " +
                        "            order by t_ms desc, seq desc " +
                        "        ) as rn " +
                        "    from " + OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di " +
                        "), " +
                        "frame_latest as ( " +
                        "    select run_id, drone_id, frame_t_ms, x, y, z " +
                        "    from frame_base " +
                        "    where rn = 1 " +
                        "), " +
                        "hotspot_pair as ( " +
                        "    select " +
                        "        a.run_id, " +
                        "        a.frame_t_ms, " +
                        "        cast(floor(((a.x + b.x) / 2.0) / " + hotspotCellSize + ") as bigint) as cell_x, " +
                        "        cast(floor(((a.y + b.y) / 2.0) / " + hotspotCellSize + ") as bigint) as cell_y, " +
                        "        cast(floor(((a.z + b.z) / 2.0) / " + hotspotCellSize + ") as bigint) as cell_z, " +
                        "        sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2) + pow(a.z - b.z, 2)) as near_dist " +
                        "    from frame_latest a " +
                        "    join frame_latest b " +
                        "      on a.run_id = b.run_id " +
                        "     and a.frame_t_ms = b.frame_t_ms " +
                        "     and a.drone_id < b.drone_id " +
                        "     and abs(a.x - b.x) <= " + hotspotNearDist + " " +
                        "     and abs(a.y - b.y) <= " + hotspotNearDist + " " +
                        "     and abs(a.z - b.z) <= " + hotspotNearDist + " " +
                        "    where sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2) + pow(a.z - b.z, 2)) <= " + hotspotNearDist + " " +
                        "), " +
                        "hotspot_cell_run as ( " +
                        "    select " +
                        "        run_id, cell_x, cell_y, cell_z, count(*) as hit_cnt " +
                        "    from hotspot_pair " +
                        "    group by run_id, cell_x, cell_y, cell_z " +
                        "), " +
                        "hotspot_run as ( " +
                        "    select " +
                        "        run_id, " +
                        "        sum(hit_cnt) as hotspot_hit_cnt, " +
                        "        count(*) as hotspot_cell_cnt, " +
                        "        case when sum(hit_cnt) = 0 then 0.0 else max(hit_cnt) * 1.0 / sum(hit_cnt) end as hotspot_top1_ratio " +
                        "    from hotspot_cell_run " +
                        "    group by run_id " +
                        "), " +
                        "run_metric as ( " +
                        "    select " +
                        "        rd.dt, " +
                        "        rd.run_id, " +
                        "        rd.scenario_id, " +
                        "        rd.strategy_id, " +
                        "        rd.strategy_version, " +
                        "        rd.param_set_id, " +
                        "        rd.param_json, " +
                        "        rd.drone_cnt, " +
                        "        rd.duration_ms, " +
                        "        rd.status, " +
                        "        rd.overspeed_cnt, " +
                        "        rd.overacc_cnt, " +
                        "        rd.teleport_cnt, " +
                        "        rd.base_v_max, " +
                        "        rd.base_a_max, " +
                        "        rd.base_teleport_dist_m, " +
                        "        coalesce(c.collision_event_cnt, 0) as collision_event_cnt, " +
                        "        coalesce(c.collision_frames_cnt, 0) as collision_frames_cnt, " +
                        "        coalesce(c.collision_min_dist, 999999.0) as collision_min_dist, " +
                        "        coalesce(c.collision_avg_dist, 999999.0) as collision_avg_dist, " +
                        "        coalesce(c.collision_avg_duration_ms, 0.0) as collision_avg_duration_ms, " +
                        "        coalesce(h.hotspot_hit_cnt, 0) as hotspot_hit_cnt, " +
                        "        coalesce(h.hotspot_cell_cnt, 0) as hotspot_cell_cnt, " +
                        "        coalesce(h.hotspot_top1_ratio, 0.0) as hotspot_top1_ratio, " +
                        "        coalesce(mr.speed_p95, 0.0) as speed_p95, " +
                        "        coalesce(mr.abs_acc_p95, 0.0) as abs_acc_p95, " +
                        "        coalesce(mr.speed_std, 0.0) as speed_std, " +
                        "        coalesce(mr.abs_acc_std, 0.0) as abs_acc_std " +
                        "    from run_dim rd " +
                        "    left join collision_run c on rd.run_id = c.run_id " +
                        "    left join hotspot_run h on rd.run_id = h.run_id " +
                        "    left join motion_run mr on rd.run_id = mr.run_id " +
                        "), " +
                        "hotspot_cell_group as ( " +
                        "    select " +
                        "        rd.dt, " +
                        "        rd.scenario_id, " +
                        "        rd.strategy_id, " +
                        "        rd.strategy_version, " +
                        "        rd.param_set_id, " +
                        "        h.cell_x, h.cell_y, h.cell_z, " +
                        "        sum(h.hit_cnt) as grp_hit_cnt " +
                        "    from hotspot_cell_run h " +
                        "    join run_dim rd on h.run_id = rd.run_id " +
                        "    group by rd.dt, rd.scenario_id, rd.strategy_id, rd.strategy_version, rd.param_set_id, h.cell_x, h.cell_y, h.cell_z " +
                        "), " +
                        "hotspot_topn_ranked as ( " +
                        "    select *, " +
                        "           row_number() over ( " +
                        "               partition by dt, scenario_id, strategy_id, strategy_version, param_set_id " +
                        "               order by grp_hit_cnt desc, cell_x asc, cell_y asc, cell_z asc " +
                        "           ) as rk " +
                        "    from hotspot_cell_group " +
                        "), " +
                        "hotspot_topn_json as ( " +
                        "    select " +
                        "        dt, scenario_id, strategy_id, strategy_version, param_set_id, " +
                        "        to_json( " +
                        "            transform( " +
                        "                sort_array(collect_list(named_struct('rk', rk, 'cell_x', cell_x, 'cell_y', cell_y, 'cell_z', cell_z, 'hit_cnt', grp_hit_cnt))), " +
                        "                x -> named_struct('cell_x', x.cell_x, 'cell_y', x.cell_y, 'cell_z', x.cell_z, 'hit_cnt', x.hit_cnt) " +
                        "            ) " +
                        "        ) as top_hotspot_json " +
                        "    from hotspot_topn_ranked " +
                        "    where rk <= 5 " +
                        "    group by dt, scenario_id, strategy_id, strategy_version, param_set_id " +
                        "), " +
                        "group_eval as ( " +
                        "    select " +
                        "        dt, scenario_id, strategy_id, strategy_version, param_set_id, " +
                        "        max(param_json) as param_json, " +
                        "        count(*) as sample_run_cnt, " +
                        "        sum(case when status = 'success' then 1 else 0 end) as success_run_cnt, " +
                        "        avg(drone_cnt) as avg_drone_cnt, " +
                        "        avg(duration_ms) as avg_duration_ms, " +
                        "        avg(base_v_max) as base_v_max, " +
                        "        avg(base_a_max) as base_a_max, " +
                        "        avg(base_teleport_dist_m) as base_teleport_dist_m, " +
                        "        avg(collision_event_cnt) as avg_collision_event_cnt, " +
                        "        avg(collision_frames_cnt) as avg_collision_frames_cnt, " +
                        "        min(collision_min_dist) as group_min_collision_dist, " +
                        "        avg(collision_avg_dist) as avg_collision_avg_dist, " +
                        "        avg(collision_avg_duration_ms) as avg_collision_avg_duration_ms, " +
                        "        avg(hotspot_hit_cnt) as avg_hotspot_hit_cnt, " +
                        "        avg(hotspot_cell_cnt) as avg_hotspot_cell_cnt, " +
                        "        avg(hotspot_top1_ratio) as avg_hotspot_top1_ratio, " +
                        "        avg(overspeed_cnt) as avg_overspeed_cnt, " +
                        "        avg(overacc_cnt) as avg_overacc_cnt, " +
                        "        avg(teleport_cnt) as avg_teleport_cnt, " +
                        "        avg(speed_p95) as avg_speed_p95, " +
                        "        avg(abs_acc_p95) as avg_abs_acc_p95, " +
                        "        avg(speed_std) as avg_speed_std, " +
                        "        avg(abs_acc_std) as avg_abs_acc_std " +
                        "    from run_metric " +
                        "    group by dt, scenario_id, strategy_id, strategy_version, param_set_id " +
                        ") " +
                        "select " +
                        "    g.dt, " +
                        "    g.scenario_id, " +
                        "    g.strategy_id, " +
                        "    g.strategy_version, " +
                        "    g.param_set_id, " +
                        "    g.param_json, " +
                        "    g.sample_run_cnt, " +
                        "    g.success_run_cnt, " +
                        "    cast(case when g.sample_run_cnt = 0 then 0.0 else g.success_run_cnt * 1.0 / g.sample_run_cnt end as double) as success_rate, " +
                        "    cast(g.avg_drone_cnt as double) as avg_drone_cnt, " +
                        "    cast(g.avg_duration_ms as double) as avg_duration_ms, " +
                        "    cast(g.base_v_max as double) as base_v_max, " +
                        "    cast(g.base_a_max as double) as base_a_max, " +
                        "    cast(g.base_teleport_dist_m as double) as base_teleport_dist_m, " +
                        "    cast(g.avg_collision_event_cnt as double) as avg_collision_event_cnt, " +
                        "    cast(g.avg_collision_frames_cnt as double) as avg_collision_frames_cnt, " +
                        "    cast(g.group_min_collision_dist as double) as group_min_collision_dist, " +
                        "    cast(g.avg_collision_avg_dist as double) as avg_collision_avg_dist, " +
                        "    cast(g.avg_collision_avg_duration_ms as double) as avg_collision_avg_duration_ms, " +
                        "    cast(g.avg_hotspot_hit_cnt as double) as avg_hotspot_hit_cnt, " +
                        "    cast(g.avg_hotspot_cell_cnt as double) as avg_hotspot_cell_cnt, " +
                        "    cast(g.avg_hotspot_top1_ratio as double) as avg_hotspot_top1_ratio, " +
                        "    coalesce(hj.top_hotspot_json, '[]') as top_hotspot_json, " +
                        "    cast(g.avg_overspeed_cnt as double) as avg_overspeed_cnt, " +
                        "    cast(g.avg_overacc_cnt as double) as avg_overacc_cnt, " +
                        "    cast(g.avg_teleport_cnt as double) as avg_teleport_cnt, " +
                        "    cast(g.avg_speed_p95 as double) as avg_speed_p95, " +
                        "    cast(g.avg_abs_acc_p95 as double) as avg_abs_acc_p95, " +
                        "    cast(g.avg_speed_std as double) as avg_speed_std, " +
                        "    cast(g.avg_abs_acc_std as double) as avg_abs_acc_std, " +
                        "    cast(g.avg_collision_event_cnt * " + wCollisionEvent + " + g.avg_collision_frames_cnt * " + wCollisionFrame + " + greatest(0.0, " + collisionDist + " - g.group_min_collision_dist) * " + wCollisionDistGap + " as double) as collision_score, " +
                        "    cast(g.avg_hotspot_hit_cnt * " + wHotspotHit + " + g.avg_hotspot_cell_cnt * " + wHotspotCell + " + g.avg_hotspot_top1_ratio * " + wHotspotTop1Ratio + " as double) as hotspot_score, " +
                        "    cast(g.avg_overspeed_cnt * " + wOverspeed + " + g.avg_overacc_cnt * " + wOveracc + " + g.avg_teleport_cnt * " + wTeleport + " + g.avg_speed_p95 * " + wSpeedP95 + " + g.avg_abs_acc_p95 * " + wAbsAccP95 + " as double) as motion_score, " +
                        "    cast((g.avg_collision_event_cnt * " + wCollisionEvent + " + g.avg_collision_frames_cnt * " + wCollisionFrame + " + greatest(0.0, " + collisionDist + " - g.group_min_collision_dist) * " + wCollisionDistGap + ") + (g.avg_hotspot_hit_cnt * " + wHotspotHit + " + g.avg_hotspot_cell_cnt * " + wHotspotCell + " + g.avg_hotspot_top1_ratio * " + wHotspotTop1Ratio + ") + (g.avg_overspeed_cnt * " + wOverspeed + " + g.avg_overacc_cnt * " + wOveracc + " + g.avg_teleport_cnt * " + wTeleport + " + g.avg_speed_p95 * " + wSpeedP95 + " + g.avg_abs_acc_p95 * " + wAbsAccP95 + ") as double) as total_score, " +
                        "    case " +
                        "        when ((g.avg_collision_event_cnt * " + wCollisionEvent + " + g.avg_collision_frames_cnt * " + wCollisionFrame + " + greatest(0.0, " + collisionDist + " - g.group_min_collision_dist) * " + wCollisionDistGap + ") + (g.avg_hotspot_hit_cnt * " + wHotspotHit + " + g.avg_hotspot_cell_cnt * " + wHotspotCell + " + g.avg_hotspot_top1_ratio * " + wHotspotTop1Ratio + ") + (g.avg_overspeed_cnt * " + wOverspeed + " + g.avg_overacc_cnt * " + wOveracc + " + g.avg_teleport_cnt * " + wTeleport + " + g.avg_speed_p95 * " + wSpeedP95 + " + g.avg_abs_acc_p95 * " + wAbsAccP95 + ")) <= " + levelAMax + " then 'A' " +
                        "        when ((g.avg_collision_event_cnt * " + wCollisionEvent + " + g.avg_collision_frames_cnt * " + wCollisionFrame + " + greatest(0.0, " + collisionDist + " - g.group_min_collision_dist) * " + wCollisionDistGap + ") + (g.avg_hotspot_hit_cnt * " + wHotspotHit + " + g.avg_hotspot_cell_cnt * " + wHotspotCell + " + g.avg_hotspot_top1_ratio * " + wHotspotTop1Ratio + ") + (g.avg_overspeed_cnt * " + wOverspeed + " + g.avg_overacc_cnt * " + wOveracc + " + g.avg_teleport_cnt * " + wTeleport + " + g.avg_speed_p95 * " + wSpeedP95 + " + g.avg_abs_acc_p95 * " + wAbsAccP95 + ")) <= " + levelBMax + " then 'B' " +
                        "        when ((g.avg_collision_event_cnt * " + wCollisionEvent + " + g.avg_collision_frames_cnt * " + wCollisionFrame + " + greatest(0.0, " + collisionDist + " - g.group_min_collision_dist) * " + wCollisionDistGap + ") + (g.avg_hotspot_hit_cnt * " + wHotspotHit + " + g.avg_hotspot_cell_cnt * " + wHotspotCell + " + g.avg_hotspot_top1_ratio * " + wHotspotTop1Ratio + ") + (g.avg_overspeed_cnt * " + wOverspeed + " + g.avg_overacc_cnt * " + wOveracc + " + g.avg_teleport_cnt * " + wTeleport + " + g.avg_speed_p95 * " + wSpeedP95 + " + g.avg_abs_acc_p95 * " + wAbsAccP95 + ")) <= " + levelCMax + " then 'C' " +
                        "        else 'D' " +
                        "    end as eval_level, " +
                        "    case " +
                        "        when g.avg_teleport_cnt > 0 then 'CHECK_DATA_QUALITY' " +
                        "        when g.avg_collision_event_cnt > 0 and g.avg_hotspot_top1_ratio >= 0.30 then 'OPTIMIZE_MATCH_AND_AVOID_HOTSPOT' " +
                        "        when g.avg_collision_event_cnt > 0 then 'STRENGTHEN_COLLISION_AVOIDANCE' " +
                        "        when g.avg_hotspot_top1_ratio >= 0.30 then 'AVOID_HOTSPOT_REGION' " +
                        "        when g.avg_overacc_cnt > g.avg_overspeed_cnt and g.avg_overacc_cnt >= 5 then 'SMOOTH_ACCELERATION' " +
                        "        when g.avg_overspeed_cnt >= 5 then 'REDUCE_SPEED' " +
                        "        else 'KEEP_CURRENT' " +
                        "    end as suggestion_type, " +
                        "    to_json(named_struct('sample_run_cnt', g.sample_run_cnt, 'success_rate', case when g.sample_run_cnt = 0 then 0.0 else g.success_run_cnt * 1.0 / g.sample_run_cnt end, 'avg_collision_event_cnt', g.avg_collision_event_cnt, 'avg_collision_frames_cnt', g.avg_collision_frames_cnt, 'group_min_collision_dist', g.group_min_collision_dist, 'avg_hotspot_hit_cnt', g.avg_hotspot_hit_cnt, 'avg_hotspot_cell_cnt', g.avg_hotspot_cell_cnt, 'avg_hotspot_top1_ratio', g.avg_hotspot_top1_ratio, 'avg_overspeed_cnt', g.avg_overspeed_cnt, 'avg_overacc_cnt', g.avg_overacc_cnt, 'avg_teleport_cnt', g.avg_teleport_cnt, 'avg_speed_p95', g.avg_speed_p95, 'avg_abs_acc_p95', g.avg_abs_acc_p95)) as suggestion_reason_json, " +
                        "    unix_timestamp() * 1000 as update_ms " +
                        "from group_eval g " +
                        "left join hotspot_topn_json hj " +
                        "    on g.dt = hj.dt " +
                        "   and g.scenario_id = hj.scenario_id " +
                        "   and g.strategy_id = hj.strategy_id " +
                        "   and g.strategy_version = hj.strategy_version " +
                        "   and g.param_set_id = hj.param_set_id";

        Dataset<Row> result = spark.sql(sql);

        System.out.println("===== ads_control_eval_di schema =====");
        result.printSchema();
        result.orderBy("dt", "scenario_id", "strategy_id", "param_set_id").show(100, false);

        result.write()
                .mode(SaveMode.Overwrite)
                .parquet(OfflineJobConfig.ADS_CONTROL_EVAL_PATH);

        HiveTableManager.createAdsControlEvalTable(spark);

        spark.sql("select * from " + OfflineJobConfig.DB_NAME +
                        ".ads_control_eval_di order by dt, scenario_id, strategy_id, param_set_id limit 100")
                .show(false);
    }
}

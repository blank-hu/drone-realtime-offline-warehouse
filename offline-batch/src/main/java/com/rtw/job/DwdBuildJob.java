package com.rtw.job;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.HiveTableManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * DWD 构建：
 * - 读取 ods_run_meta_di / ods_traj_point_di
 * - 复刻实时 DwdDiffWithMetaFn 的核心规则：
 *   dt_ms / dx / dy / dz / dist / speed / acc
 *   is_time_back / is_teleport / is_overspeed / is_overacc
 *
 * 当前版本先把“点级差分 + 风险标记”做准，后续再往上做 DWS。
 */
public class DwdBuildJob {

    public void run(SparkSession spark) {
        HiveTableManager.initDatabase(spark);

        Dataset<Row> odsMeta = spark.table(OfflineJobConfig.DB_NAME + ".ods_run_meta_di");
        Dataset<Row> odsPoint = spark.table(OfflineJobConfig.DB_NAME + ".ods_traj_point_di");

        odsMeta.createOrReplaceTempView("tmp_ods_run_meta_di");
        odsPoint.createOrReplaceTempView("tmp_ods_traj_point_di");

        String sql =
                "with base as ( " +
                        "   select " +
                        "       p.run_id, p.drone_id, p.seq, p.point_key, p.t_ms, p.x, p.y, p.z, " +
                        "       p.scenario_id, p.strategy_id, p.strategy_version, p.param_set_id, p.seed, " +
                        "       m.v_max, m.a_max, m.teleport_dist_m, " +
                        "       lag(p.t_ms) over(partition by p.run_id, p.drone_id order by p.seq) as prev_t_ms, " +
                        "       lag(p.x) over(partition by p.run_id, p.drone_id order by p.seq) as prev_x, " +
                        "       lag(p.y) over(partition by p.run_id, p.drone_id order by p.seq) as prev_y, " +
                        "       lag(p.z) over(partition by p.run_id, p.drone_id order by p.seq) as prev_z " +
                        "   from tmp_ods_traj_point_di p " +
                        "   left join tmp_ods_run_meta_di m " +
                        "     on p.run_id = m.run_id " +
                        "), diff as ( " +
                        "   select *, " +
                        "       case when prev_t_ms is null then null else (t_ms - prev_t_ms) end as dt_ms, " +
                        "       case when prev_x is null then null else (x - prev_x) end as dx, " +
                        "       case when prev_y is null then null else (y - prev_y) end as dy, " +
                        "       case when prev_z is null then null else (z - prev_z) end as dz, " +
                        "       case when prev_x is null then null " +
                        "            else sqrt(pow(x - prev_x, 2) + pow(y - prev_y, 2) + pow(z - prev_z, 2)) end as dist " +
                        "   from base " +
                        "), speed_calc as ( " +
                        "   select *, " +
                        "       case " +
                        "           when prev_t_ms is null then null " +
                        "           when dt_ms <= 0 then null " +
                        "           else dist / (dt_ms / 1000.0) " +
                        "       end as speed " +
                        "   from diff " +
                        "), acc_calc as ( " +
                        "   select *, " +
                        "       lag(speed) over(partition by run_id, drone_id order by seq) as prev_speed " +
                        "   from speed_calc " +
                        ") " +
                        "select " +
                        "   run_id, drone_id, seq, point_key, t_ms, x, y, z, " +
                        "   scenario_id, strategy_id, strategy_version, param_set_id, seed, " +
                        "   prev_t_ms, prev_x, prev_y, prev_z, " +
                        "   dt_ms, dx, dy, dz, dist, speed, " +
                        "   case " +
                        "       when prev_speed is null or dt_ms is null or dt_ms <= 0 then null " +
                        "       else (speed - prev_speed) / (dt_ms / 1000.0) " +
                        "   end as acc, " +
                        "   v_max, a_max, teleport_dist_m, " +
                        "   case when dt_ms is not null and dt_ms <= 0 then 1 else 0 end as is_time_back, " +
                        "   case " +
                        "       when dt_ms is null or dt_ms <= 0 then 0 " +
                        "       when dist > teleport_dist_m then 1 else 0 " +
                        "   end as is_teleport, " +
                        "   case " +
                        "       when dt_ms is null or dt_ms <= 0 then 0 " +
                        "       when speed > v_max then 1 else 0 " +
                        "   end as is_overspeed, " +
                        "   case " +
                        "       when dt_ms is null or dt_ms <= 0 then 0 " +
                        "       when abs(case when prev_speed is null then null else (speed - prev_speed) / (dt_ms / 1000.0) end) > a_max then 1 else 0 " +
                        "   end as is_overacc, " +
                        "   0 as is_stuck " +
                        "from acc_calc";

        Dataset<Row> dwdDf = spark.sql(sql);

        System.out.println("===== dwd_traj_point_detail_di schema =====");
        dwdDf.printSchema();
        dwdDf.orderBy("drone_id", "seq").show(100, false);

        dwdDf.write()
                .mode(SaveMode.Overwrite)
                .parquet(OfflineJobConfig.DWD_TRAJ_POINT_PATH);

        HiveTableManager.createDwdTrajPointTable(spark);

        spark.sql("select * from " + OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di order by drone_id, seq limit 100")
                .show(false);
    }
}

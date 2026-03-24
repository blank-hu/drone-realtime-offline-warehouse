package com.rtw.job;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.HiveTableManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

public class DwsRunQualityBuildJob {

    public void run(SparkSession spark) {
        HiveTableManager.initDatabase(spark);
        spark.conf().set("spark.sql.session.timeZone", "UTC");

        Dataset<Row> odsMeta = spark.table(OfflineJobConfig.DB_NAME + ".ods_run_meta_di");
        Dataset<Row> dwdPoint = spark.table(OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di");

        odsMeta.createOrReplaceTempView("tmp_ods_run_meta_di");
        dwdPoint.createOrReplaceTempView("tmp_dwd_traj_point_detail_di");

        final int topN = OfflineJobConfig.DWS_TOP_N;
        final int wOs = OfflineJobConfig.SCORE_W_OS;
        final int wOa = OfflineJobConfig.SCORE_W_OA;
        final int wTp = OfflineJobConfig.SCORE_W_TP;

        String sql =
                "with dwd_base as ( " +
                        "  select " +
                        "    run_id, drone_id, t_ms, " +
                        "    scenario_id, strategy_id, strategy_version, param_set_id, seed, " +
                        "    cast(coalesce(is_overspeed, 0) as bigint) as os, " +
                        "    cast(coalesce(is_overacc, 0) as bigint) as oa, " +
                        "    cast(coalesce(is_teleport, 0) as bigint) as tp " +
                        "  from tmp_dwd_traj_point_detail_di " +
                        "), " +
                        "run_agg as ( " +
                        "  select " +
                        "    run_id, " +
                        "    min(t_ms) as min_t_ms, " +
                        "    max(t_ms) as max_t_ms, " +
                        "    sum(os) as overspeed_cnt, " +
                        "    sum(oa) as overacc_cnt, " +
                        "    sum(tp) as teleport_cnt, " +
                        "    count(distinct drone_id) as seen_drone_cnt, " +
                        "    max(scenario_id) as dwd_scenario_id, " +
                        "    max(strategy_id) as dwd_strategy_id, " +
                        "    max(strategy_version) as dwd_strategy_version, " +
                        "    max(param_set_id) as dwd_param_set_id, " +
                        "    max(seed) as dwd_seed " +
                        "  from dwd_base " +
                        "  group by run_id " +
                        "), " +
                        "drone_agg as ( " +
                        "  select " +
                        "    run_id, drone_id, " +
                        "    sum(os) as overspeed, " +
                        "    sum(oa) as overacc, " +
                        "    sum(tp) as teleport, " +
                        "    cast(sum(os) * " + wOs + " + sum(oa) * " + wOa + " + sum(tp) * " + wTp + " as bigint) as score " +
                        "  from dwd_base " +
                        "  group by run_id, drone_id " +
                        "), " +
                        "topn_ranked as ( " +
                        "  select *, row_number() over(partition by run_id order by score desc, drone_id asc) as rk " +
                        "  from drone_agg " +
                        "), " +
                        "topn_json as ( " +
                        "  select " +
                        "    run_id, " +
                        "    to_json( " +
                        "      transform( " +
                        "        sort_array(collect_list(named_struct( " +
                        "          'rk', rk, " +
                        "          'drone_id', drone_id, " +
                        "          'overspeed', overspeed, " +
                        "          'overacc', overacc, " +
                        "          'teleport', teleport, " +
                        "          'score', score " +
                        "        ))), " +
                        "        x -> named_struct( " +
                        "          'drone_id', x.drone_id, " +
                        "          'overspeed', x.overspeed, " +
                        "          'overacc', x.overacc, " +
                        "          'teleport', x.teleport, " +
                        "          'score', x.score " +
                        "        ) " +
                        "      ) " +
                        "    ) as top_drones_json " +
                        "  from topn_ranked " +
                        "  where rk <= " + topN + " " +
                        "  group by run_id " +
                        ") " +
                        "select " +
                        "  case when coalesce(m.start_ms, a.min_t_ms) >= 1000000000000 " +
                        "       then to_date(from_unixtime(cast(coalesce(m.start_ms, a.min_t_ms) / 1000 as bigint))) " +
                        "       else null end as dt, " +
                        "  a.run_id, " +
                        "  coalesce(m.scenario_id, a.dwd_scenario_id) as scenario_id, " +
                        "  coalesce(m.strategy_id, a.dwd_strategy_id) as strategy_id, " +
                        "  coalesce(m.strategy_version, a.dwd_strategy_version) as strategy_version, " +
                        "  coalesce(m.param_set_id, a.dwd_param_set_id) as param_set_id, " +
                        "  coalesce(m.seed, a.dwd_seed) as seed, " +
                        "  coalesce(m.start_ms, a.min_t_ms) as start_ms, " +
                        "  coalesce(m.end_ms, a.max_t_ms) as end_ms, " +
                        "  coalesce(m.duration_ms, coalesce(m.end_ms, a.max_t_ms) - coalesce(m.start_ms, a.min_t_ms)) as duration_ms, " +
                        "  coalesce(m.status, 'success') as status, " +
                        "  a.overspeed_cnt, " +
                        "  a.overacc_cnt, " +
                        "  a.teleport_cnt, " +
                        "  cast(coalesce(m.drone_count, cast(a.seen_drone_cnt as int)) as int) as drone_cnt, " +
                        "  coalesce(t.top_drones_json, '[]') as top_drones_json, " +
                        "  to_json(named_struct('w_os', " + wOs + ", 'w_oa', " + wOa + ", 'w_tp', " + wTp + ")) as score_weights_json " +
                        "from run_agg a " +
                        "left join tmp_ods_run_meta_di m on a.run_id = m.run_id " +
                        "left join topn_json t on a.run_id = t.run_id";

        Dataset<Row> result = spark.sql(sql)
                .withColumn("update_ms", lit(System.currentTimeMillis()))
                .withColumn("is_final", lit(1))
                .withColumn("snapshot_ms", lit(System.currentTimeMillis()));

        System.out.println("===== dws_run_quality_di schema =====");
        result.printSchema();
        result.show(false);

        result.write().mode(SaveMode.Overwrite).parquet(OfflineJobConfig.DWS_RUN_QUALITY_PATH);
        HiveTableManager.createDwsRunQualityTable(spark);
        spark.sql("select * from " + OfflineJobConfig.DB_NAME + ".dws_run_quality_di").show(false);
    }
}

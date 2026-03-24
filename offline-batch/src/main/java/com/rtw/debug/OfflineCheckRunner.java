package com.rtw.debug;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OfflineCheckRunner {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("offline-check-runner");
        try {
            spark.sql("use " + OfflineJobConfig.DB_NAME);
            spark.conf().set("spark.sql.session.timeZone", "UTC");

            // =========================
            // 1. 先看核心结果表
            // =========================
            runQuery(spark, "查看 ods_run_meta_di",
                    "select * from drone_dw.ods_run_meta_di",
                    20);

            runQuery(spark, "查看 dwd_traj_point_detail_di 前 20 条",
                    "select * from drone_dw.dwd_traj_point_detail_di order by run_id, drone_id, seq limit 20",
                    20);

            runQuery(spark, "查看 dws_run_quality_di",
                    "select * from drone_dw.dws_run_quality_di",
                    20);

            runQuery(spark, "查看 dws_overspeed_event_di 前 50 条",
                    "select * from drone_dw.dws_overspeed_event_di order by run_id, drone_id, start_seq limit 50",
                    50);

            runQuery(spark, "查看 dws_collision_event_di 前 50 条",
                    "select * from drone_dw.dws_collision_event_di order by run_id, drone_a, drone_b, start_t_ms limit 50",
                    50);

            // =========================
            // 2. 离线内部校验：overspeed_event
            // =========================
            runAggQuery(spark, "overspeed_event 唯一性统计",
                    "select count(*) as total_cnt, " +
                            "count(distinct concat(run_id, '|', event_id)) as distinct_cnt " +
                            "from drone_dw.dws_overspeed_event_di");

            runExpectZero(spark, "overspeed_event 边界合法性（应为空）",
                    "select * " +
                            "from drone_dw.dws_overspeed_event_di " +
                            "where start_seq > end_seq " +
                            "   or start_t_ms > end_t_ms " +
                            "   or duration_ms <> end_t_ms - start_t_ms " +
                            "   or points_cnt < 3");

            runExpectZero(spark, "overspeed_event 起点坐标回查（应为空）",
                    "select " +
                            "    e.run_id, e.drone_id, e.event_id " +
                            "from drone_dw.dws_overspeed_event_di e " +
                            "join drone_dw.dwd_traj_point_detail_di p " +
                            "  on e.run_id = p.run_id " +
                            " and e.drone_id = p.drone_id " +
                            " and e.start_seq = p.seq " +
                            "where e.start_x <> p.x " +
                            "   or e.start_y <> p.y " +
                            "   or e.start_z <> p.z");

            runExpectZero(spark, "overspeed_event 终点坐标回查（应为空）",
                    "select " +
                            "    e.run_id, e.drone_id, e.event_id " +
                            "from drone_dw.dws_overspeed_event_di e " +
                            "join drone_dw.dwd_traj_point_detail_di p " +
                            "  on e.run_id = p.run_id " +
                            " and e.drone_id = p.drone_id " +
                            " and e.end_seq = p.seq " +
                            "where e.end_x <> p.x " +
                            "   or e.end_y <> p.y " +
                            "   or e.end_z <> p.z");

            runExpectZero(spark, "overspeed_event points_cnt 回查（应为空）",
                    "select " +
                            "    e.run_id, e.drone_id, e.event_id, e.points_cnt, count(*) as actual_os_points " +
                            "from drone_dw.dws_overspeed_event_di e " +
                            "join drone_dw.dwd_traj_point_detail_di p " +
                            "  on e.run_id = p.run_id " +
                            " and e.drone_id = p.drone_id " +
                            " and p.seq between e.start_seq and e.end_seq " +
                            " and p.is_overspeed = 1 " +
                            "group by e.run_id, e.drone_id, e.event_id, e.points_cnt " +
                            "having e.points_cnt <> count(*)");

            runExpectZero(spark, "overspeed_event 相邻事件可再合并校验（应为空）",
                    "with x as ( " +
                            "    select *, " +
                            "           lead(start_seq) over(partition by run_id, drone_id order by start_seq) as next_start_seq " +
                            "    from drone_dw.dws_overspeed_event_di " +
                            ") " +
                            "select * from x " +
                            "where next_start_seq is not null " +
                            "  and next_start_seq - end_seq <= 2");

            runQuery(spark, "run_quality 与 overspeed_event 总量关系",
                    "select " +
                            "    q.run_id, " +
                            "    q.overspeed_cnt as run_quality_os_cnt, " +
                            "    coalesce(sum(e.points_cnt), 0) as event_os_points " +
                            "from drone_dw.dws_run_quality_di q " +
                            "left join drone_dw.dws_overspeed_event_di e " +
                            "  on q.run_id = e.run_id " +
                            "group by q.run_id, q.overspeed_cnt",
                    20);

            // =========================
            // 3. 离线内部校验：collision_event
            // =========================
            runAggQuery(spark, "collision_event 唯一性统计",
                    "select count(*) as total_cnt, " +
                            "count(distinct concat(run_id, '|', drone_a, '|', drone_b, '|', cast(start_t_ms as string))) as distinct_cnt " +
                            "from drone_dw.dws_collision_event_di");

            runExpectZero(spark, "collision_event 合法性校验（应为空）",
                    "select * " +
                            "from drone_dw.dws_collision_event_di " +
                            "where drone_a >= drone_b " +
                            "   or start_t_ms > end_t_ms " +
                            "   or duration_ms <> end_t_ms - start_t_ms " +
                            "   or frames_cnt <= 0 " +
                            "   or min_dist > d_min");

            runExpectZero(spark, "collision_event 相邻事件可再合并校验（应为空）",
                    "with x as ( " +
                            "    select *, " +
                            "           lag(end_t_ms) over(partition by run_id, drone_a, drone_b order by start_t_ms) as prev_end_t_ms " +
                            "    from drone_dw.dws_collision_event_di " +
                            ") " +
                            "select * from x " +
                            "where prev_end_t_ms is not null " +
                            "  and start_t_ms - prev_end_t_ms <= " + OfflineJobConfig.TRAJ_FRAME_MS);

            runQuery(spark, "collision_event 按 pair 统计",
                    "select run_id, drone_a, drone_b, count(*) as event_cnt, sum(frames_cnt) as frames_sum " +
                            "from drone_dw.dws_collision_event_di " +
                            "group by run_id, drone_a, drone_b " +
                            "order by frames_sum desc " +
                            "limit 50",
                    50);

            // =========================
            // 4. 实时 vs 离线对账
            // =========================
            if (OfflineJobConfig.ENABLE_RT_RECONCILE) {
                registerDorisTable(spark,
                        OfflineJobConfig.DORIS_DB + ".dws_run_quality_v1",
                        "rt_dws_run_quality_v1");

                registerDorisTable(spark,
                        OfflineJobConfig.DORIS_DB + ".dws_overspeed_event_v1",
                        "rt_dws_overspeed_event_v1");

                registerDorisTable(spark,
                        OfflineJobConfig.DORIS_DB + ".dws_collision_event_v1",
                        "rt_dws_collision_event_v1");

                // 4.1 run_quality 精确对账
                runExpectZero(spark, "实时 vs 离线 run_quality 对账（应为空）",
                        "select " +
                                "    coalesce(o.run_id, r.run_id) as run_id, " +
                                "    o.overspeed_cnt as offline_overspeed_cnt, r.overspeed_cnt as rt_overspeed_cnt, " +
                                "    o.overacc_cnt as offline_overacc_cnt, r.overacc_cnt as rt_overacc_cnt, " +
                                "    o.teleport_cnt as offline_teleport_cnt, r.teleport_cnt as rt_teleport_cnt, " +
                                "    o.drone_cnt as offline_drone_cnt, r.drone_cnt as rt_drone_cnt " +
                                "from drone_dw.dws_run_quality_di o " +
                                "full outer join rt_dws_run_quality_v1 r " +
                                "  on o.run_id = r.run_id " +
                                "where o.run_id is null or r.run_id is null " +
                                "   or o.overspeed_cnt <> r.overspeed_cnt " +
                                "   or o.overacc_cnt <> r.overacc_cnt " +
                                "   or o.teleport_cnt <> r.teleport_cnt " +
                                "   or o.drone_cnt <> r.drone_cnt");

                // 4.2 overspeed_event 聚合对账
                runExpectZero(spark, "实时 vs 离线 overspeed_event 聚合对账（应为空）",
                        "with off_agg as ( " +
                                "    select run_id, drone_id, count(*) as evt_cnt, sum(points_cnt) as pt_cnt " +
                                "    from drone_dw.dws_overspeed_event_di " +
                                "    group by run_id, drone_id " +
                                "), rt_agg as ( " +
                                "    select run_id, drone_id, count(*) as evt_cnt, sum(points_cnt) as pt_cnt " +
                                "    from rt_dws_overspeed_event_v1 " +
                                "    group by run_id, drone_id " +
                                ") " +
                                "select " +
                                "    coalesce(o.run_id, r.run_id) as run_id, " +
                                "    coalesce(o.drone_id, r.drone_id) as drone_id, " +
                                "    o.evt_cnt as offline_evt_cnt, r.evt_cnt as rt_evt_cnt, " +
                                "    o.pt_cnt as offline_pt_cnt, r.pt_cnt as rt_pt_cnt " +
                                "from off_agg o " +
                                "full outer join rt_agg r " +
                                "  on o.run_id = r.run_id and o.drone_id = r.drone_id " +
                                "where o.run_id is null or r.run_id is null " +
                                "   or o.evt_cnt <> r.evt_cnt " +
                                "   or o.pt_cnt <> r.pt_cnt");

                // 4.3 collision_event 聚合对账
                runExpectZero(spark, "实时 vs 离线 collision_event 聚合对账（应为空）",
                        "with off_agg as ( " +
                                "    select run_id, drone_a, drone_b, count(*) as evt_cnt, sum(frames_cnt) as frames_sum " +
                                "    from drone_dw.dws_collision_event_di " +
                                "    group by run_id, drone_a, drone_b " +
                                "), rt_agg as ( " +
                                "    select run_id, drone_a, drone_b, count(*) as evt_cnt, sum(frames_cnt) as frames_sum " +
                                "    from rt_dws_collision_event_v1 " +
                                "    group by run_id, drone_a, drone_b " +
                                ") " +
                                "select " +
                                "    coalesce(o.run_id, r.run_id) as run_id, " +
                                "    coalesce(o.drone_a, r.drone_a) as drone_a, " +
                                "    coalesce(o.drone_b, r.drone_b) as drone_b, " +
                                "    o.evt_cnt as offline_evt_cnt, r.evt_cnt as rt_evt_cnt, " +
                                "    o.frames_sum as offline_frames_sum, r.frames_sum as rt_frames_sum " +
                                "from off_agg o " +
                                "full outer join rt_agg r " +
                                "  on o.run_id = r.run_id and o.drone_a = r.drone_a and o.drone_b = r.drone_b " +
                                "where o.run_id is null or r.run_id is null " +
                                "   or o.evt_cnt <> r.evt_cnt " +
                                "   or o.frames_sum <> r.frames_sum");
            } else {
                System.out.println("\n==============================");
                System.out.println("未开启 Doris 实时对账");
                System.out.println("如需对账，请把 OfflineJobConfig.ENABLE_RT_RECONCILE 改成 true，并配置 Doris JDBC。");
                System.out.println("==============================\n");
            }

        } finally {
            spark.stop();
        }
    }

    private static void registerDorisTable(SparkSession spark, String tableName, String viewName) {
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", OfflineJobConfig.DORIS_JDBC_URL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", tableName)
                .option("user", OfflineJobConfig.DORIS_USER)
                .option("password", OfflineJobConfig.DORIS_PASSWORD)
                .load();
        df.createOrReplaceTempView(viewName);
    }

    private static void runQuery(SparkSession spark, String title, String sql, int showRows) {
        System.out.println("\n==============================");
        System.out.println(title);
        System.out.println("------------------------------");
        System.out.println(sql);
        System.out.println("------------------------------");
        Dataset<Row> df = spark.sql(sql);
        df.printSchema();
        df.show(showRows, false);
    }

    private static void runAggQuery(SparkSession spark, String title, String sql) {
        runQuery(spark, title, sql, 20);
    }

    private static void runExpectZero(SparkSession spark, String title, String sql) {
        System.out.println("\n==============================");
        System.out.println(title);
        System.out.println("------------------------------");
        System.out.println(sql);
        System.out.println("------------------------------");

        Dataset<Row> df = spark.sql(sql);
        long cnt = df.count();

        if (cnt == 0) {
            System.out.println("[PASS] 结果为空，校验通过");
        } else {
            System.out.println("[FAIL] 发现 " + cnt + " 条异常记录");
            df.show(50, false);
        }
    }
}
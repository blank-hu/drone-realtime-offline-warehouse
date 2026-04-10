package com.rtw.debug;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.HiveTableManager;
import com.rtw.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Debug：查询最小间距 TopN（默认 Top30）
 *
 * 口径与离线碰撞构建 DwsCollisionEventBuildJob 保持一致：
 * 1) 先按 frame_t_ms = floor(t_ms / frameMs) * frameMs 归帧
 * 2) 同一帧内每个 drone 只保留 t_ms 最大的一条点
 * 3) 同一帧内做两两配对，且仅保留 drone_a < drone_b，避免重复
 * 4) dist = 三维欧式距离
 *
 * 用法：
 *   直接运行 main()，默认查全量 run 的 Top30
 *   也可以传参：args[0]=runId, args[1]=limit
 *   例如：run_001 30
 */
public class Top30MinDistanceDebugJob {

    public void run(SparkSession spark, String runId, int limit) {
        HiveTableManager.initDatabase(spark);
        spark.conf().set("spark.sql.session.timeZone", "UTC");

        Dataset<Row> dwdPoint = spark.table(OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di");
        dwdPoint.createOrReplaceTempView("tmp_dwd_traj_point_detail_di");

        final long frameMs = OfflineJobConfig.TRAJ_FRAME_MS;
        final String runFilter = (runId == null || runId.isBlank())
                ? ""
                : " where run_id = '" + runId.replace("'", "''") + "' ";

        String sql =
                "with src as ( " +
                        "  select * " +
                        "  from tmp_dwd_traj_point_detail_di " +
                        runFilter +
                        "), frame_base as ( " +
                        "  select " +
                        "    run_id, drone_id, seq, t_ms, x, y, z, " +
                        "    strategy_id, strategy_version, param_set_id, seed, " +
                        "    cast(floor(t_ms / " + ((double) frameMs) + ") * " + frameMs + " as bigint) as frame_t_ms, " +
                        "    row_number() over ( " +
                        "      partition by run_id, cast(floor(t_ms / " + ((double) frameMs) + ") * " + frameMs + " as bigint), drone_id " +
                        "      order by t_ms desc, seq desc " +
                        "    ) as rn " +
                        "  from src " +
                        "), frame_latest as ( " +
                        "  select " +
                        "    run_id, drone_id, seq, t_ms, x, y, z, " +
                        "    strategy_id, strategy_version, param_set_id, seed, frame_t_ms " +
                        "  from frame_base " +
                        "  where rn = 1 " +
                        "), pair_dist as ( " +
                        "  select " +
                        "    a.run_id, " +
                        "    a.frame_t_ms, " +
                        "    a.drone_id as drone_a, " +
                        "    b.drone_id as drone_b, " +
                        "    sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2) + pow(a.z - b.z, 2)) as dist, " +
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
                        ") " +
                        "select " +
                        "  run_id, " +
                        "  frame_t_ms, " +
                        "  drone_a, " +
                        "  drone_b, " +
                        "  dist, " +
                        "  ax, ay, az, " +
                        "  bx, by, bz, " +
                        "  strategy_id, strategy_version, param_set_id, seed " +
                        "from pair_dist " +
                        "order by dist asc, frame_t_ms asc, drone_a asc, drone_b asc " +
                        "limit " + limit;

        System.out.println("===== SQL: Top" + limit + " 最小间距 =====");
        System.out.println(sql);

        Dataset<Row> result = spark.sql(sql);
        result.show(limit, false);
    }

    public static void main(String[] args) {
        String runId = args != null && args.length > 0 ? args[0] : null;
        int limit = 30;
        if (args != null && args.length > 1) {
            try {
                limit = Integer.parseInt(args[1]);
            } catch (Exception ignored) {
            }
        }

        SparkSession spark = SparkSessionFactory.create("debug-top-min-distance");
        try {
            new Top30MinDistanceDebugJob().run(spark, runId, limit);
        } finally {
            spark.stop();
        }
    }
}

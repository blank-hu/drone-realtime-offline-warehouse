package com.rtw.util;

import com.rtw.config.OfflineJobConfig;
import org.apache.spark.sql.SparkSession;

public class HiveTableManager {

    public static void initDatabase(SparkSession spark) {
        spark.sql("create database if not exists " + OfflineJobConfig.DB_NAME);
        spark.sql("use " + OfflineJobConfig.DB_NAME);
    }

    public static void createOdsRunMetaTable(SparkSession spark) {
        spark.sql(
                "create table if not exists " + OfflineJobConfig.DB_NAME + ".ods_run_meta_di (" +
                        "run_id string, " +
                        "scenario_id string, " +
                        "strategy_id string, " +
                        "strategy_version string, " +
                        "param_json string, " +
                        "param_set_id string, " +
                        "seed int, " +
                        "start_ms bigint, " +
                        "end_ms bigint, " +
                        "status string, " +
                        "drone_count int, " +
                        "duration_ms bigint, " +
                        "v_max double, " +
                        "a_max double, " +
                        "teleport_dist_m double" +
                        ") using parquet " +
                        "location '" + OfflineJobConfig.ODS_RUN_META_PATH.replace("\\", "/") + "'"
        );
    }

    public static void createOdsTrajPointTable(SparkSession spark) {
        spark.sql(
                "create table if not exists " + OfflineJobConfig.DB_NAME + ".ods_traj_point_di (" +
                        "run_id string, " +
                        "drone_id string, " +
                        "seq int, " +
                        "t_ms bigint, " +
                        "x double, " +
                        "y double, " +
                        "z double, " +
                        "vx double, " +
                        "vy double, " +
                        "vz double, " +
                        "ax double, " +
                        "ay double, " +
                        "az double, " +
                        "status string, " +
                        "scenario_id string, " +
                        "strategy_id string, " +
                        "strategy_version string, " +
                        "param_set_id string, " +
                        "seed int, " +
                        "point_key string" +
                        ") using parquet " +
                        "location '" + OfflineJobConfig.ODS_TRAJ_POINT_PATH.replace("\\", "/") + "'"
        );
    }

    public static void createDwdTrajPointTable(SparkSession spark) {
        spark.sql(
                "create table if not exists " + OfflineJobConfig.DB_NAME + ".dwd_traj_point_detail_di (" +
                        "run_id string, " +
                        "drone_id string, " +
                        "seq int, " +
                        "point_key string, " +
                        "t_ms bigint, " +
                        "x double, y double, z double, " +
                        "scenario_id string, " +
                        "strategy_id string, " +
                        "strategy_version string, " +
                        "param_set_id string, " +
                        "seed int, " +
                        "prev_t_ms bigint, " +
                        "prev_x double, prev_y double, prev_z double, " +
                        "dt_ms bigint, " +
                        "dx double, dy double, dz double, " +
                        "dist double, " +
                        "speed double, " +
                        "acc double, " +
                        "v_max double, " +
                        "a_max double, " +
                        "teleport_dist_m double, " +
                        "is_time_back int, " +
                        "is_teleport int, " +
                        "is_overspeed int, " +
                        "is_overacc int, " +
                        "is_stuck int" +
                        ") using parquet " +
                        "location '" + OfflineJobConfig.DWD_TRAJ_POINT_PATH.replace("\\", "/") + "'"
        );
    }

    public static void createDwsRunQualityTable(SparkSession spark) {
        spark.sql(
                "create table if not exists " + OfflineJobConfig.DB_NAME + ".dws_run_quality_di (" +
                        "dt date, " +
                        "run_id string, " +
                        "scenario_id string, " +
                        "strategy_id string, " +
                        "strategy_version string, " +
                        "param_set_id string, " +
                        "seed int, " +
                        "start_ms bigint, " +
                        "end_ms bigint, " +
                        "duration_ms bigint, " +
                        "status string, " +
                        "overspeed_cnt bigint, " +
                        "overacc_cnt bigint, " +
                        "teleport_cnt bigint, " +
                        "drone_cnt int, " +
                        "top_drones_json string, " +
                        "update_ms bigint, " +
                        "is_final int, " +
                        "snapshot_ms bigint, " +
                        "score_weights_json string" +
                        ") using parquet " +
                        "location '" + OfflineJobConfig.DWS_RUN_QUALITY_PATH.replace("\\", "/") + "'"
        );
    }

    public static void createDwsOverspeedEventTable(SparkSession spark) {
        spark.sql(
                "create table if not exists " + OfflineJobConfig.DB_NAME + ".dws_overspeed_event_di (" +
                        "dt date, " +
                        "run_id string, " +
                        "scenario_id string, " +
                        "drone_id string, " +
                        "event_id string, " +
                        "start_seq int, " +
                        "end_seq int, " +
                        "start_t_ms bigint, " +
                        "end_t_ms bigint, " +
                        "duration_ms bigint, " +
                        "points_cnt int, " +
                        "max_speed double, " +
                        "avg_speed double, " +
                        "start_x double, " +
                        "start_y double, " +
                        "start_z double, " +
                        "end_x double, " +
                        "end_y double, " +
                        "end_z double, " +
                        "strategy_id string, " +
                        "strategy_version string, " +
                        "param_set_id string, " +
                        "seed int, " +
                        "update_ms bigint" +
                        ") using parquet " +
                        "location '" + OfflineJobConfig.DWS_OVERSPEED_EVENT_PATH.replace("\\", "/") + "'"
        );
    }
    public static void createDwsCollisionEventTable(SparkSession spark) {
        spark.sql(
                "create table if not exists " + OfflineJobConfig.DB_NAME + ".dws_collision_event_di (" +
                        "dt date, " +
                        "run_id string, " +
                        "drone_a string, " +
                        "drone_b string, " +
                        "start_t_ms bigint, " +
                        "event_id string, " +
                        "end_t_ms bigint, " +
                        "duration_ms bigint, " +
                        "frames_cnt int, " +
                        "min_dist double, " +
                        "avg_dist double, " +
                        "d_min double, " +
                        "start_ax double, " +
                        "start_ay double, " +
                        "start_az double, " +
                        "start_bx double, " +
                        "start_by double, " +
                        "start_bz double, " +
                        "end_ax double, " +
                        "end_ay double, " +
                        "end_az double, " +
                        "end_bx double, " +
                        "end_by double, " +
                        "end_bz double, " +
                        "strategy_id string, " +
                        "strategy_version string, " +
                        "param_set_id string, " +
                        "seed int, " +
                        "update_ms bigint" +
                        ") using parquet " +
                        "location '" + OfflineJobConfig.DWS_COLLISION_EVENT_PATH.replace("\\", "/") + "'"
        );
    }
    
}

package com.rtw;

import com.rtw.util.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class ResetHiveTablesMain {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("reset-hive");
        try {
            spark.sql("create database if not exists drone_dw");
            spark.sql("use drone_dw");

            spark.sql("drop table if exists ods_run_meta");
            spark.sql("drop table if exists ods_traj_point");
            spark.sql("drop table if exists dwd_traj_point_detail");

            spark.sql("drop table if exists ods_run_meta_di");
            spark.sql("drop table if exists ods_traj_point_di");
            spark.sql("drop table if exists dwd_traj_point_detail_di");
            spark.sql("drop table if exists dws_run_quality_di");

            spark.sql("show tables").show(false);
        } finally {
            spark.stop();
        }
    }
}
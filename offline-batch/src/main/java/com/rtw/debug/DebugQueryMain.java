package com.rtw.debug;

import com.rtw.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DebugQueryMain {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("debug-query");

        Dataset<Row> odsMeta = spark.read()
                .parquet("D:/warehouse/offline-batch/data/warehouse/ods_run_meta_di");

        Dataset<Row> odsPoint = spark.read()
                .parquet("D:/warehouse/offline-batch/data/warehouse/ods_traj_point_di");

        Dataset<Row> dwd = spark.read()
                .parquet("D:/warehouse/offline-batch/data/warehouse/dwd_traj_point_detail_di");

        Dataset<Row> dws = spark.read()
                .parquet("D:/warehouse/offline-batch/data/warehouse/dws_run_quality_di");

        System.out.println("===== ods_run_meta_di =====");
        odsMeta.show(false);

        System.out.println("===== ods_traj_point_di =====");
        odsPoint.show(20, false);

        System.out.println("===== dwd_traj_point_detail_di =====");
        dwd.show(20, false);

        System.out.println("===== dws_run_quality_di =====");
        dws.show(20, false);

        spark.stop();
    }
}
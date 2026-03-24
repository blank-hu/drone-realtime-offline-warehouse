package com.rtw.util;

import com.rtw.config.OfflineJobConfig;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    public static SparkSession create(String appName) {
        System.setProperty("hadoop.home.dir", "D:\\hadoop");

        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.sql.warehouse.dir", OfflineJobConfig.WAREHOUSE_DIR)
                .enableHiveSupport()
                .getOrCreate();
    }
}
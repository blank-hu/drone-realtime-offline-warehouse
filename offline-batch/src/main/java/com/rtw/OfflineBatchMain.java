package com.rtw;

import com.rtw.job.DwdBuildJob;
import com.rtw.job.DwsCollisionEventBuildJob;
import com.rtw.job.DwsOverspeedEventBuildJob;
import com.rtw.job.DwsRunQualityBuildJob;
import com.rtw.job.OdsBuildJob;
import com.rtw.util.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class OfflineBatchMain {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("offline-batch-main");
        try {
            new OdsBuildJob().run(spark);
            new DwdBuildJob().run(spark);
            new DwsOverspeedEventBuildJob().run(spark);
            new DwsCollisionEventBuildJob().run(spark);
            new DwsRunQualityBuildJob().run(spark);
        } finally {
            spark.stop();
        }
    }
}
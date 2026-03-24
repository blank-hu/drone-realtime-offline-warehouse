package com.rtw.debug;

import com.rtw.config.OfflineJobConfig;
import com.rtw.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OfflineSqlViewer {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("offline-sql-viewer");
        try {
            // 使用你的数仓库
            spark.sql("use " + OfflineJobConfig.DB_NAME);



            runQuery(spark, "碰撞段异常数据检查",
                    "select *\n" +
                            "from drone_dw.dws_collision_event_di\n" +
                            "order by run_id, drone_a, drone_b, start_t_ms\n" +
                            "limit 50;",
                    100);



        } finally {
            spark.stop();
        }
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
}
package com.rtw;


import com.rtw.config.AppConfig;
import com.rtw.model.CollisionHit;
import com.rtw.pipelines.PipelineDwd;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;

import java.util.*;


public class Main {
    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.fromArgsEnv(args);
        Queue<int[] > pro = new PriorityQueue<int[] >((int[] a, int[] b) -> a[0] - b[0]);

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8081); // UI 端口，默认就是 8081

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(cfg.flinkParallelism);

        // 强烈建议开发期启用 checkpoint（也会促使 sink2 定期 flush）
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        PipelineDwd.build(env, cfg);
        env.execute("rt-warehouse-uav-dwd");
    }
}

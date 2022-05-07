package com.djin.gmallrealtime.app;

import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.djin.gmallrealtime.utils.KafkaUtil;
import com.djin.gmallrealtime.utils.MySQLSourceUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RealTimeWithFlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1 创建mySqlSource
        DebeziumSourceFunction mySQLSource = MySQLSourceUtil.getMySQLSource();
        // 2 配置执行环境，Flink-CDC将读取binlog的位置信息以状态的方式保存再CK，如果想要做断点续传，需要从Checkpoint或者Savepoint启动程序
        // 2.1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.2 开启Checkpoint,每隔5秒钟做一次CK
        env.enableCheckpointing(5000L);
        // 2.3 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 2.4 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        // 2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node001:8020/flinkCDC");
        //env.setStateBackend(new FsStateBackend("hdfs://node001:8082/flinkCDC"));过期
        // 2.7 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "djin");
        // 3 创建Flink-MySQL-CDC的Source
        DataStreamSource<String> mySQL_source = env.addSource(mySQLSource);
        mySQL_source.setParallelism(1);
        // 4 打印数据
//        mySQL_source.print().setParallelism(1);
        //4.推送数据到kafka
        mySQL_source.addSink(KafkaUtil.getKafkaSink("ods_base_db"));
        // 5 启动任务
        env.execute("MyFlinkCDC");
    }
}

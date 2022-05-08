package com.djin.gmallrealtime.app;

import com.alibaba.fastjson.JSONObject;
import com.djin.gmallrealtime.common.NewUserCheckFunction;
import com.djin.gmallrealtime.common.SideOutputFunction;
import com.djin.gmallrealtime.utils.KafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * Flink调用工具类读取数据的主程序
 * @author dj
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
       // 1.获取执行环境，设置并行度，开启CK，设置状态后端（HDFS）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // 为Kafka主题的分区数
        env.setParallelism(1);
       // 1.1 设置状态后端
       // env.setStateBackend(new FsStateBackend("hdfs://node001:8020/gmall/dwd_log/ck"));
       // 1.2 开启CK
       // env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
       // env.getCheckpointConfig().setCheckpointTimeout(60000L);
       // 修改用户名
        System.setProperty("HADOOP_USER_NAME","djin");
       // 2.读取Kafka ods_base_log 主题数据
        String topic = "ods_base_topic";
        String groupId = "ods_dwd_base_log_app";
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
       // 3.将每行数据转换为JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);
       // 打印测试
       // jsonObjDS.print();
       // 4. 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(data ->
                data.getJSONObject("common").getString("mid"));
       // 5. 使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(
                new NewUserCheckFunction());
       // 打印测试
       // jsonWithNewFlagDS.print();
       // 6.分流，使用ProcessFunction将ODS数据拆分成启动、曝光以及页面数据
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(
                new SideOutputFunction());
       // 7.将三个流的数据写入对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {});
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") {});
        pageDS.addSink(KafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(KafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(KafkaUtil.getKafkaSink("dwd_display_log"));
       // 打印测试
        pageDS.print("Page>>>>>>");
        startDS.print("Start>>>>>>");
        displayDS.print("Display>>>>>>");

       // 执行任务
        env.execute();
    }
}

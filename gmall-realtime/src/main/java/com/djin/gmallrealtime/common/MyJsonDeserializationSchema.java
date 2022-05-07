package com.djin.gmallrealtime.common;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyJsonDeserializationSchema implements DebeziumDeserializationSchema {
    // 自定义数据解析器
    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        //获取主题信息，包含数据库和表名:mysql_binlog_source.gmall2022.user_info
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];
        //获取操作类型 READ DELETE UPDATE CREATE
        String operation = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        //获取值信息并转换为Struct类型
        Struct value = (Struct) sourceRecord.value();
        //获取转换后的数据
        Struct after = value.getStruct("after");
        //创建JSON对象用于存储数据信息
        JSONObject data = new JSONObject();
        for (Field field : after.schema().fields()) {
            Object o = after.get(field);
            data.put(field.name(), o);
        }
        //创建JSON对象用于封装最终返回值数据信息
        JSONObject result = new JSONObject();
        result.put("operation", operation);
        result.put("data", data);
        result.put("database", db);
        result.put("table", tableName);

        //发送数据至下游
        collector.collect(result.toJSONString());
    }
}

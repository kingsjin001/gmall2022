package com.djin.gmallrealtime.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流，使用ProcessFunction将ODS数据拆分成启动、曝光以及页面数据
 * @author dj
 */
public class SideOutputFunction extends ProcessFunction<JSONObject, String> {
    @Override
    public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
        // 提取”start“字段
        String start = value.getString("start");
        // 判断是否为启动数据
        if (start != null && start.length() > 0) {
        // 将启动日志输出到侧输出流
            ctx.output(new OutputTag<String>("start") {
                       },
                    value.toString());
        } else {
            // 为页面数据，将数据输出到主流
            out.collect(value.toString());
            // 不是启动数据，继续判断是否是曝光数据
            JSONArray displays = value.getJSONArray("displays");
            if (displays != null && displays.size() > 0) {
                // 为曝光数据，遍历写入侧输出流
                for (int i = 0; i < displays.size(); i++) {
                    // 取出单条曝光数据
                    JSONObject displayJson = displays.getJSONObject(i);
                    // 添加页面ID
                    displayJson.put("page_id",
                            value.getJSONObject("page").getString("page_id"));
                    // 输出到侧输出流
                    ctx.output(new OutputTag<String>("display") {
                               },
                            displayJson.toString());
                }
            }
        }
    }
}

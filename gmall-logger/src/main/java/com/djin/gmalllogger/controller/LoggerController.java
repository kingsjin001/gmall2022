package com.djin.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test1() {
        System.out.println("success");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nm,
                        @RequestParam(value = "age", defaultValue = "23") int age) {
        System.out.println(nm + ":" + age);
        return "success";
    }

    @RequestMapping("applog")
    public String geting(@RequestParam("param") String jsonStr) {
        //打印数据
        System.out.println(jsonStr);
        //将数据落盘
//        log.debug(jsonStr);
        log.info(jsonStr);
//        log.warn(jsonStr);
//        log.error(jsonStr);
//        log.trace(jsonStr);

        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}

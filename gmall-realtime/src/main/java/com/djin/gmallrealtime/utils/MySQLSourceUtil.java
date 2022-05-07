package com.djin.gmallrealtime.utils;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.djin.gmallrealtime.common.MyJsonDeserializationSchema;

public class MySQLSourceUtil {
    private static DebeziumSourceFunction mySqlSource = MySQLSource.<String>builder()
            .hostname("node001")
            .port(3306)
            .databaseList("gmall2022")
//                .tableList("gmall2022.user_info")  //可选配置项,如果不指定该参数,则会
            //读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.latest())
            .deserializer(new MyJsonDeserializationSchema())
            .build();

    public static DebeziumSourceFunction getMySQLSource() {
        return mySqlSource;
    }
}

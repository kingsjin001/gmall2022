package com.djin.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlCDC {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.创建FlinkSql-MySql-CDC的source
        tableEnv.executeSql("create table user_info(" +
                "id int," +
                "name string," +
                "phone_num string" +
                ")with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='node001'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='123456'," +
                "'database-name'='gmall2022'," +
                "'table-name'='user_info')");
        TableResult tableResult = tableEnv.executeSql("select * from user_info");
        tableResult.print();
        env.execute("FlinkSql-CDC");


    }
}

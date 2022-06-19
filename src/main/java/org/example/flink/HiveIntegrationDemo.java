package org.example.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveIntegrationDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "mydb";
        String hiveConfDir = "/home/mengh/flink_standalone/flink-1.14.2/conf/hive-conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        // 使用注册的catalog
        tableEnv.useCatalog("myhive");
        // 向Hive表中写入一条数据
        String insertSQL = "insert into student select 1,'zs','m',23,'cs'";

        TableResult result2 = tableEnv.executeSql(insertSQL);
        System.out.println(result2.getJobClient().get().getJobStatus());

    }
}
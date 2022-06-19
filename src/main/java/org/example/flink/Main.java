package org.example.flink;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 参考：https://blog.csdn.net/qq_36062467/article/details/119203333
 * kafka to hive demo
 */
public class Main {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);


        String name = "myhive";
        String defaultDatabase = "mydb";
        String hiveConfDir = "/home/mengh/flink_standalone/flink-1.14.2/conf/hive-conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);


        tableEnv.useCatalog(name);
        tableEnv.executeSql("drop table if exists kafka_source");
        tableEnv.executeSql("CREATE TABLE kafka_source (\n" +
                "   alarmId string,\n" +
                "   alarmLevel string,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'mh_kfk_source',\n" +
                "  'properties.bootstrap.servers' = '10.100.2.191:6667',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "   'json.fail-on-missing-field' = 'false',\n" +
                "   'json.ignore-parse-errors' = 'true')");

        //	切换到hive方言创建hive表,auto-compaction和compaction.file-size设置hive小文件合并
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("drop table if exists hive_sink");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS hive_sink\n" +
                "(\n" +
                "alarmId STRING,\n" +
                "alarmLevel STRING\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING, hr STRING)\n" +
                "STORED AS parquet TBLPROPERTIES (\n" +
                "'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                "'sink.partition-commit.trigger'='partition-time',\n" +
                "'sink.partition-commit.delay'='0s',\n" +
                "'sink.partition-commit.policy.kind'='metastore,success-file',\n" +
                "'auto-compaction'='true',\n" +
                "'compaction.file-size'='128MB'\n" +
                ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // 	向Hive表中写入数据
        tableEnv.executeSql("insert into hive_sink select alarmId,alarmLevel,DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH') from kafka_source");

    }
}

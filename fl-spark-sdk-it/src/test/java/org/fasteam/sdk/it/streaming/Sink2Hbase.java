package org.fasteam.sdk.it.streaming;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.log.HbaseLog;
import org.fasteam.kafka.KafkaEnvironmentContext;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.fasteam.streaming.entry.HbaseBatchWriter;
import scala.collection.Map;

import java.util.Arrays;
import java.util.UUID;

/**
 * Description:  org.fasteam.sdk.it.streaming
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/17
 */
public class Sink2Hbase extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        SparkProcessor.getContext().setLogLevel("warn");
        //构造数据源表的schema
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("name",DataTypes.StringType,true),
                DataTypes.createStructField("age",DataTypes.StringType,true),
                DataTypes.createStructField("value",DataTypes.StringType,true)));
        StructType targetSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("value",DataTypes.StringType,true)));
        JSONObject timestamp = new JSONObject();
        timestamp.put("0",1694490871000L);
        timestamp.put("1",1694490871000L);
        JSONObject topic = new JSONObject();
        topic.put("kafka_test_ods",timestamp);
        KafkaEnvironmentContext kafkaEnvironmentContext = (KafkaEnvironmentContext)EnvironmentContextFactory.get(KafkaEnvironmentContext.class);
        Dataset<Row> ds = SparkProcessor.getSession().readStream().format("kafka")
                .option("kafka.bootstrap.servers",kafkaEnvironmentContext.getBootstrap())
                .option("subscribe","kafka_test_ods")
                .option("startingOffsetsByTimestamp",topic.toJSONString())
                .load();
        ds = ds.selectExpr("CAST(value AS STRING) as dl_json");
        ds = ds.select(functions.from_json(ds.col("dl_json"),schema).as("dl_data")).select("dl_data.*");
        ds = ds.selectExpr("*","UUID() as rowkey");
//        //写hbase
        ds.writeStream().foreachBatch(new HbaseBatchWriter("sink_test_dwd","cf")).start();
        ds = ds.toJSON().map(new MapFunction<String, Row>() {
            @Override
            public Row call(String value) throws Exception {
                return RowFactory.create(value);
            }
        }, RowEncoder.apply(targetSchema));
        ds.writeStream().format("console").outputMode("append").option("checkpointLocation","/checkpoint").start();
        //还需要写kafka
        ds.writeStream().format("kafka")
                .option("kafka.bootstrap.servers",kafkaEnvironmentContext.getBootstrap())
                .option("topic","sink_test_dws")
                .option("checkpointLocation","/checkpoint1")
                .start();
        SparkProcessor.getSession().streams().awaitAnyTermination();
    }
}

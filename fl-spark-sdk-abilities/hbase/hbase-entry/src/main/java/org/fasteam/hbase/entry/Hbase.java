package org.fasteam.hbase.entry;

import com.alibaba.fastjson.JSONObject;
import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.fasteam.hbase.entry.level2index.DefaultMapFromPutFunction;
import org.fasteam.hbase.entry.level2index.Level2IndexBody;
import org.fasteam.hbase.entry.level2index.Level2IndexContext;
import org.fasteam.hbase.entry.effkey.ConsistentHashing;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;
import scala.Tuple2;

import java.io.IOException;
import java.util.UUID;

/**
 * Description:  org.fasteam.hbase.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/6/28
 */
public class Hbase{
    public static JavaRDD<Tuple2<ImmutableBytesWritable, Result>> rddScan(String tableName, Scan scan) throws IOException {
        Configuration configuration = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature().getConfiguration();
        configuration.set(TableInputFormat.INPUT_TABLE, tableName.trim());
        configuration.set(TableInputFormat.SCAN_ROW_START, Bytes.toString(scan.getStartRow()));
        configuration.set(TableInputFormat.SCAN_ROW_STOP, Bytes.toString(scan.getStopRow()));
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        configuration.set(TableInputFormat.SCAN, ScanToString);
        JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkProcessor.getContext(), configuration);
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> result = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan, SaltTableInputFormat.class, (Function<Tuple2<ImmutableBytesWritable, Result>, Tuple2<ImmutableBytesWritable, Result>>) v1 -> v1);
        return result;
    }

    public static JavaRDD<Tuple2<ImmutableBytesWritable, Result>> rddNativeScan(String tableName,Scan scan){
        Configuration configuration = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature().getConfiguration();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkProcessor.getContext(), configuration);
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> result = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan);
        return result;
    }
    public static JavaRDD<Result> rowGet(String tableName,JavaRDD<byte[]>keys){
        Configuration configuration = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature().getConfiguration();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkProcessor.getContext(), configuration);
        return hbaseContext.bulkGet(TableName.valueOf(tableName), 20, keys, Get::new, v1 -> v1);
    }
    public static void rddWrite(String tableName, JavaRDD<Put> rdd){
        HbaseEnvironmentContext.ContextHolder context = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkProcessor.getContext(), context.getConfiguration());
//        Config config = Config.getInstance();
//        String zkQuorum = config.props.get(String.class,HbaseEnvironmentParameter.ZOOKEEPER_STRING);
        ConsistentHashing hashCreator = new ConsistentHashing();
        //处理rdd中的rowkey，增加预分区的盐
        rdd = rdd.map(new ReviseRowkeyFunction());
//        //获取锁，分布式加锁写入
//        EffKeyLock lock = new EffKeyLock(tableName, 5000, () -> zkQuorum);
//        lock.lock();
        hbaseContext.bulkPut(rdd,TableName.valueOf(tableName),f->f);
//        lock.unlock();

        if(context.isAutoLogged()) {
            logged(tableName,rdd,hbaseContext);
        }
    }

    public static void rddWriteWithIndex(String tableName, JavaRDD<Put> putRdd, String... indexColumnNames){
        HbaseEnvironmentContext.ContextHolder context = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkProcessor.getContext(), context.getConfiguration());
        //处理rdd中的rowkey，增加预分区的盐
        putRdd = putRdd.map(new ReviseRowkeyFunction());
        hbaseContext.bulkPut(putRdd,TableName.valueOf(tableName),f->f);
        //写入二级索引
        JavaRDD<String> indexRdd = putRdd.flatMap(new DefaultMapFromPutFunction(indexColumnNames)).map(new Function<Level2IndexBody, String>() {
            @Override
            public String call(Level2IndexBody v1) throws Exception {
                JSONObject jsonBody = new JSONObject();
                jsonBody.put(v1.getIndexColumn(),v1.getColumnValue());
                jsonBody.put(Level2IndexContext.INDEX_KEY_COLUMN_NAME,v1.getSearchKey());
                return jsonBody.toString();
            }
        });
        JavaEsSpark.saveJsonToEs(indexRdd,tableName);

        //日志写入
        if(context.isAutoLogged()) {
            logged(tableName,putRdd,hbaseContext);
        }
    }


    public static void rddWriteWithIndex(String tableName, JavaRDD<Put> putRdd, FlatMapFunction<Put, Level2IndexBody> putLevel2IndexFunction){
        HbaseEnvironmentContext.ContextHolder context = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkProcessor.getContext(), context.getConfiguration());
        //处理rdd中的rowkey，增加预分区的盐
        putRdd = putRdd.map(new ReviseRowkeyFunction());
        hbaseContext.bulkPut(putRdd,TableName.valueOf(tableName),f->f);
        //写入二级索引
        JavaRDD<String> indexRdd = putRdd.flatMap(putLevel2IndexFunction).map(new Function<Level2IndexBody, String>() {
            @Override
            public String call(Level2IndexBody v1) throws Exception {
                JSONObject jsonBody = new JSONObject();
                jsonBody.put(v1.getIndexColumn(),v1.getColumnValue());
                jsonBody.put(Level2IndexContext.INDEX_KEY_COLUMN_NAME,v1.getSearchKey());
                return jsonBody.toString();
            }
        });
        JavaEsSpark.saveJsonToEs(indexRdd,tableName);

        //日志写入
        if(context.isAutoLogged()) {
            logged(tableName,putRdd,hbaseContext);
        }
    }
    private static void logged(String tableName, JavaRDD<Put> putRdd, JavaHBaseContext hbaseContext){
        String logTableName = String.format("%s_log", tableName);
        JavaRDD<Put> rddLog = putRdd.map((Function<Put, Put>) put -> {
            String logKey = Bytes.toString(put.getRow());
            String rowKey = String.format("%s:%s", SparkProcessor.getBatchId(), UUID.randomUUID());
            Put logPut = new Put(Bytes.toBytes(rowKey));
            put.getFamilyCellMap().forEach((k, v) -> {
                v.stream().forEach(cell -> {
                    logPut.addColumn(Bytes.toBytes("self"), Bytes.toBytes("table_key"), Bytes.toBytes(logKey));
                    //log簇
                    logPut.addColumn(Bytes.toBytes("log"), Bytes.toBytes("log_batch_no"), Bytes.toBytes(SparkProcessor.getBatchId()));
                    logPut.addColumn(Bytes.toBytes("log"), Bytes.toBytes("user_id"), Bytes.toBytes(SparkProcessor.getUserId()));
                });
            });
            return logPut;
        });
        hbaseContext.bulkPut(rddLog, TableName.valueOf(logTableName), f -> f);
    }

    static class ReviseRowkeyFunction implements Function<Put, Put>{
        ConsistentHashing hashCreator;

        public ReviseRowkeyFunction() {
            this.hashCreator = new ConsistentHashing();
        }
        @Override
        public Put call(Put put) throws Exception {
            String rowKey = Bytes.toString(put.getRow());
            rowKey = String.format("%s:%s",hashCreator.selectNode(rowKey),rowKey);
            Put newPut = new Put(Bytes.toBytes(rowKey));
            put.getFamilyCellMap().forEach((k,v)->{
                v.stream().forEach(cell->{
                    newPut.addColumn(
                            Bytes.toBytes(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())),
                            Bytes.toBytes(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength())),
                            Bytes.toBytes(Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength())));
                });
            });
            return newPut;
        }
    }
}

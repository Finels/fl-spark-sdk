package org.fasteam.streaming.entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.HbaseEnvironmentContext;
import org.fasteam.hbase.entry.log.HbaseLog;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;
import scala.Function1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Description:  org.fasteam.streaming.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/15
 */
public class HbaseBatchWriter implements VoidFunction2<Dataset<Row>, Long>{
    String targetTableName;
    String defaultColumnFamilyName;

    public HbaseBatchWriter(String targetTableName, String defaultColumnFamilyName) {
        this.targetTableName = targetTableName;
        this.defaultColumnFamilyName = defaultColumnFamilyName;
    }

    //每个微批次的
    @Override
    public void call(Dataset<Row> dataset, Long batchId) throws Exception {
        Hbase.rddWrite(targetTableName,dataset.toJavaRDD().map(new HbaseRow2PutTranslator(defaultColumnFamilyName)));
    }
}

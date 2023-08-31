package org.fasteam.sdk.it.level2index;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.fasteam.hbase.entry.level2index.Level2IndexBody;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.log.HbaseLog;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Description:  org.fasteam.sdk.it.level2index
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/16
 */
public class WriteTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        //生成数据
        Put put = new Put(Bytes.toBytes(System.currentTimeMillis()+ UUID.randomUUID().toString()));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("value"),Bytes.toBytes("test"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes("10"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes("50"));
        Hbase.rddWriteWithIndex("hbase_assemble_test", SparkProcessor.getContext().parallelize(ImmutableList.<Put>of(put)),"name");
    }
}

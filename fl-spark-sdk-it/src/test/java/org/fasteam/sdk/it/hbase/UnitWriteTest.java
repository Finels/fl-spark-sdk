package org.fasteam.sdk.it.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.log.HbaseLog;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;

import java.util.Arrays;
import java.util.UUID;

/**
 * Description:  org.fasteam.sdk.example.hbase
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/4
 */
public class UnitWriteTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        //测试写入单元测试表
        //构造两条数据，主键符合字典顺序但不连续，用于后续的加盐读取测试
        //记录日志
        HbaseLog hbaseLog = new HbaseLog(UUID.randomUUID().toString());
        String rowkey1 = "0";
        String rowkey2 = "3";
        Put put = new Put(Bytes.toBytes(rowkey1));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("value"),Bytes.toBytes("中文测试"));
        Put put1 = new Put(Bytes.toBytes(rowkey2));
        put1.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("value"),Bytes.toBytes("中文测试"));
        Hbase.rddWrite("hbase_assemble_test",SparkProcessor.getContext().parallelize(Arrays.asList(put)));
        Hbase.rddWrite("hbase_assemble_test",SparkProcessor.getContext().parallelize(Arrays.asList(put1)));
    }
}

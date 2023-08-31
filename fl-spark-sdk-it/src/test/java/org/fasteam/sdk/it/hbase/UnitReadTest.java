package org.fasteam.sdk.it.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;

/**
 * Description:  org.fasteam.sdk.example.hbase
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/4
 */
public class UnitReadTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        //测试全表scan
        Hbase.rddNativeScan("hbase_assemble_test",new Scan());
        //测试非加盐扫描
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("0"));
        scan.withStopRow(Bytes.toBytes("1"));
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> nativeScanRdd = Hbase.rddNativeScan("hbase_assemble_test",scan);
        //测试加盐读取
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> saltScanRdd =Hbase.rddScan("hbase_assemble_test",scan);
        //空值校验
        Assertions.assertFalse(nativeScanRdd.isEmpty());
        Assertions.assertFalse(saltScanRdd.isEmpty());
        //数量校验
        Assertions.assertFalse(nativeScanRdd.count()>1);
        Assertions.assertFalse(saltScanRdd.count()>1);
    }
}

package org.fasteam.sdk.it.hbase;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;
import java.util.stream.Stream;

/**
 * Description:  org.fasteam.sdk.it.hbase
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/8
 */
public class QuickSaltReadTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Scan scan = new Scan();
        //过去10分钟的数据
        Calendar ca = Calendar.getInstance();
        Long stopTime = ca.getTimeInMillis();
        ca.add(Calendar.DAY_OF_MONTH,-2);
        Long startTime = ca.getTimeInMillis();
        scan.withStartRow(Bytes.toBytes(startTime+""));
        scan.withStopRow(Bytes.toBytes(stopTime+""));
        //1693099699641
        //1693209147577
        //1693272499641
        System.out.println("stopTime:"+stopTime+" , startTime:"+startTime);
        JavaRDD rdd = Hbase.rddScan("hbase_assemble_test",scan);
        System.out.println(rdd.count());
    }

}

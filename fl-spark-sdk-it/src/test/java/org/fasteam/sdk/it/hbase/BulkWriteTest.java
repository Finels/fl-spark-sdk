package org.fasteam.sdk.it.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.log.HbaseLog;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Description:  org.fasteam.sdk.it.hbase
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/8
 */
public class BulkWriteTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        // 创建数据，格式为时间戳+随机UUID
        //一共须插入100*1024*1024条数据
        //按照100*1024为一批来执行
        //记录日志
        HbaseLog hbaseLog = new HbaseLog(UUID.randomUUID().toString());
        for(int s=0;s<1024;s++) {
            List<Put> lst = new ArrayList<>();
            for (int i = 0; i < 100 * 1024; i++) {
                String rowkey = System.currentTimeMillis() + ":" + UUID.randomUUID().toString();
                Put put = new Put(Bytes.toBytes(rowkey));
                //一条数据10个字段
                for (int j = 0; j < 50; j++) {
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("value" + j), Bytes.toBytes(UUID.randomUUID().toString()));
                }
                lst.add(put);
            }
            Hbase.rddWriteWithIndex("hbase_assemble_test", SparkProcessor.getContext().parallelize(lst),"value10");
            System.out.println("完成"+(s+1)+"次插入");
        }
    }

}

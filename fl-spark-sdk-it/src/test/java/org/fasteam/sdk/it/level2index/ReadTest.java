package org.fasteam.sdk.it.level2index;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.level2index.Level2IndexContext;
import org.fasteam.hbase.entry.level2index.Level2IndexProvider;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.junit.jupiter.api.Assertions;

/**
 * Description:  org.fasteam.sdk.it.level2index
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/16
 */
public class ReadTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Level2IndexProvider provider = ((Level2IndexContext)EnvironmentContextFactory.get(Level2IndexContext.class)).getFeature();
//        JavaRDD<byte[]> rowkey1 = provider.load("hbase_assemble_test","value10='b1dc1fd3-9c2f-4f3f-8010-5ac37c78581b'");
        JavaRDD<byte[]> rowkey2 = provider.load("hbase_assemble_test","cf_age > 10");
        JavaRDD<byte[]> rowkey3 = provider.load("hbase_assemble_test","cf_name < 10");
//        Hbase.rowGet("hbase_assemble_test",rowkey1).map(new Function<Result, String>() {
//            @Override
//            public String call(Result result) throws Exception {
//                JSONObject s = new JSONObject();
//                for (Cell cell : result.listCells()) {
//                    String column = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
//                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//                    s.put(column,value);
//                }
//                return s.toJSONString();
//            }
//        }).collect().forEach(System.out::println);
//        Assertions.assertFalse(Hbase.rowGet("hbase_assemble_test",rowkey1).isEmpty());
        Assertions.assertFalse(Hbase.rowGet("hbase_assemble_test",rowkey2).isEmpty());
        Assertions.assertTrue(Hbase.rowGet("hbase_assemble_test",rowkey3).isEmpty());
    }
}

package org.fasteam.hbase.entry.level2index;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fasteam.sdk.core.SparkProcessor;

import java.io.Serializable;

/**
 * Description:  org.fasteam.aspect
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/10
 */
@Data
@AllArgsConstructor
public class EsLevel2IndexProvider extends Level2IndexProvider implements Serializable {
    private String node;
    private String port;
    private String user;
    private String pass;
    @Override
    Level2IndexContext.IndexType type() {
        return Level2IndexContext.IndexType.elasticsearch;
    }

    @Override
    public JavaRDD<byte[]> load(String indexName, String sqlConditionExpr) {
        Dataset<Row> ds = SparkProcessor.getSession().read().format("es").load(indexName);
        ds = ds.where(sqlConditionExpr);
        return ds.select(ds.col(Level2IndexContext.INDEX_KEY_COLUMN_NAME)).toJavaRDD().map(new Function<Row, byte[]>() {
            @Override
            public byte[] call(Row row) throws Exception {
                return Bytes.toBytes((String) row.getAs(Level2IndexContext.INDEX_KEY_COLUMN_NAME));
            }
        });
    }

}

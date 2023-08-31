package org.fasteam.streaming.entry;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


/**
 * Description:  org.fasteam.streaming.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/16
 */
public class HbaseRow2PutTranslator implements Function<Row, Put> {
    private String defaultRowkeyColumnName = "rowkey";
    private String columnFamilyName;

    public HbaseRow2PutTranslator(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public HbaseRow2PutTranslator(String columnFamilyName,String defaultRowkeyColumnName) {
        this.defaultRowkeyColumnName = defaultRowkeyColumnName;
        this.columnFamilyName = columnFamilyName;
    }
    @Override
    public Put call(Row row) throws Exception {
        Put put = new Put(Bytes.toBytes((String)row.getAs(defaultRowkeyColumnName)));
        for (String fieldName : row.schema().fieldNames()) {
            put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(fieldName),Bytes.toBytes((String)row.getAs(fieldName)));
        }
        return put;
    }
}

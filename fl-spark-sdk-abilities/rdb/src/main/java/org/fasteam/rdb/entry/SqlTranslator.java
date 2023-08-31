package org.fasteam.rdb.entry;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;


/**
 * Description:  org.fasteam.streaming.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/15
 */
public class SqlTranslator implements Function2<Row,String,String> {
    @Override
    public String call(Row row, String targetTableName) throws Exception {
        //目前sql语句只支持insert和update
        return "insert into " + targetTableName +
                "\n" +
                "(" +
                String.join(",", row.schema().fieldNames()) +
                ")" +
                "values(" +
                row.mkString(",") +
                ");";
    }
}

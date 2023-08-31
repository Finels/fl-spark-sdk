package org.fasteam.hbase.entry.level2index;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Description:  org.fasteam.hbase.entry.level2index
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/28
 */
//默认的将hbase的put对象转换为indexBody对象的方法
public class DefaultMapFromPutFunction implements FlatMapFunction<Put, Level2IndexBody> {
    String indexColumnNames;
    //figure out them
    public DefaultMapFromPutFunction(String[] indexColumnNames) {
        this.indexColumnNames = String.join(",", indexColumnNames);

    }
    @Override
    public Iterator<Level2IndexBody> call(Put put) throws Exception {
        List<Level2IndexBody> lst = new ArrayList<>();
        put.getFamilyCellMap().forEach((k,v)->{
            v.forEach(cell->{
                String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                if(indexColumnNames.contains(columnName)) {
                    Level2IndexBody indexBody = new Level2IndexBody();
                    indexBody.setSearchKey(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    indexBody.setIndexColumn(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
                    indexBody.setColumnValue(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    lst.add(indexBody);
                }
            });
        });
        return lst.iterator();
    }
}
package org.fasteam.hbase.entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:  用于Hbase加盐表的读取，定义读取时的rowkey分区的盐的位置
 * 必须要定义分区盐的位置，否则加盐hbase表读取出来的数据会不完整
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public class SaltTableInputFormat extends TableInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = getConf();
        String table = conf.get(TableInputFormat.INPUT_TABLE);

        TableName tableName = TableName.valueOf(table);

        Connection connection = ConnectionFactory.createConnection(conf);
        RegionLocator regionLocator = connection.getRegionLocator(tableName);

        String scanStart = conf.get(TableInputFormat.SCAN_ROW_START);
        String scanStop = conf.get(TableInputFormat.SCAN_ROW_STOP);

        Pair<byte[][], byte[][]> keys = regionLocator.getStartEndKeys();

        byte[][] firstKey = keys.getFirst();

        List<InputSplit> splits = new ArrayList<>(firstKey.length);

        for (int i = 0; i < firstKey.length; i++) {
            byte[] fb = firstKey[i];
            String rl = getTableRegionLocation(regionLocator,keys.getFirst()[i]);

            String regionSalt = null;
            if (firstKey[i].length > 0) {
                regionSalt = Bytes.toString(firstKey[i]);
            } else {
                regionSalt = "00";
            }
            byte[] startRowKey = Bytes.toBytes(regionSalt + ":" + scanStart);
            byte[] endRowKey = Bytes.toBytes(regionSalt + ":" + scanStop);

            InputSplit split = new TableSplit(tableName, startRowKey, endRowKey, rl);

            splits.add(split);
        }

        return splits;
    }

    private String getTableRegionLocation(RegionLocator regionLocator,byte[] rowkey) throws IOException {
        return regionLocator.getRegionLocation(rowkey).getHostname();
    }
}

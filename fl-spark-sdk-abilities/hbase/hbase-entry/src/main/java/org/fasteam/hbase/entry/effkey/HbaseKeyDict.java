package org.fasteam.hbase.entry.effkey;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.sdk.core.SparkProcessor;

import java.util.Arrays;
import java.util.Collections;

/**
 * Description:  应用的是表级锁
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/7/25
 */
public class HbaseKeyDict implements KeyDict{
    private final String DICT_SUFFIX="_dict";
    private EffKeyLock lock;

    public HbaseKeyDict(EffKeyLock lock) {
        this.lock = lock;
    }

    @Override
    public void putKey(String tableName, String key) {
        //开始锁表
        lock.setKey(tableName);
        lock.lock();
        //查询这个key是否已存在索引
        key = String.format("name:%s",key);
        //固定的tableName
        JavaRDD<Result> result = Hbase.rowGet(tableName+DICT_SUFFIX,SparkProcessor.getContext().parallelize(Collections.singletonList(Bytes.toBytes(key))));
        if(result.isEmpty()){
            //不存在
            //查询当前最大索引
            Hbase.rowGet(tableName+DICT_SUFFIX,SparkProcessor.getContext().parallelize(Collections.singletonList(Bytes.toBytes("0x000"))));
            //构造put对象
        }

    }

    @Override
    public Key getKey(String tableName, String key) {
        return null;
    }
}

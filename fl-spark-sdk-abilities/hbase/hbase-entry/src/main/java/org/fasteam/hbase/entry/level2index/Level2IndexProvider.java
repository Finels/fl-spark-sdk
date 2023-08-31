package org.fasteam.hbase.entry.level2index;

import org.apache.spark.api.java.JavaRDD;

/**
 * Description:  org.fasteam.aspect
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/10
 */
public abstract class Level2IndexProvider {
    abstract Level2IndexContext.IndexType type();
    public abstract JavaRDD<byte[]> load(String indexName, String sqlConditionExpr);
}

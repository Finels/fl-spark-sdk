package org.fasteam.hbase.entry.effkey;

/**
 * Description:  org.fasteam.hbase.entry.effkey
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/7/25
 */
public interface KeyDict {
    void putKey(String tableName,String key);
    Key getKey(String tableName,String key);

}

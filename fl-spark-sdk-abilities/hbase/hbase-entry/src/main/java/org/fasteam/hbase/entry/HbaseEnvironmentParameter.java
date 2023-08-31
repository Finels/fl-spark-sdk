package org.fasteam.hbase.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/**
 * Description:  org.fasteam.hbase.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/6/30
 */
public class HbaseEnvironmentParameter implements Serializable {
    public static final String ZOOKEEPER_STRING =  "abilities.hbase.zk-quorum";
    public static final String ZOOKEEPER_PORT =  "abilities.hbase.zk-port";
    public static final String ZOOKEEPER_CLIENT_TIMEOUT =  "abilities.hbase.client-timeout";
    public static final String ZOOKEEPER_RPC_TIMEOUT =  "abilities.hbase.rpc-timeout";
    public static final String AUTO_LOGGED =  "abilities.hbase.auto-logged";
}

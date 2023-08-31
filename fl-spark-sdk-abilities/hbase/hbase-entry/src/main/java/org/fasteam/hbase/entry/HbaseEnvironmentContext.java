package org.fasteam.hbase.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;

import static org.fasteam.hbase.entry.HbaseEnvironmentParameter.*;

/**
 * Description:  Hbase的环境初始化
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public class HbaseEnvironmentContext extends EnvironmentContext<HbaseEnvironmentContext.ContextHolder> {

    @Data
    @AllArgsConstructor
    public class ContextHolder{
        private Configuration configuration;
        private boolean autoLogged;
    }
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String quorum = config.props.getString(ZOOKEEPER_STRING);
        String clientPort = config.props.getString(ZOOKEEPER_PORT);
        String clientTimeout = config.props.getString(ZOOKEEPER_CLIENT_TIMEOUT);
        String timeout = config.props.getString(ZOOKEEPER_RPC_TIMEOUT);
        boolean logged = config.props.getBoolean(AUTO_LOGGED);
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",quorum);
        configuration.set("hbase.zookeeper.clientPort",clientPort);
        configuration.set("hbase.client.operation.timeout",clientTimeout);
        configuration.set("hbase.rpc.timeout",timeout);
        setFeature(new ContextHolder(configuration,logged));
        super.initialization();
    }

    @Override
    public ContextHolder callbackFeature() {
        return getFeature();
    }
}

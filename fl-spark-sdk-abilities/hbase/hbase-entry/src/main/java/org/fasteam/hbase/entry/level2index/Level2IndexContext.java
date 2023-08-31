package org.fasteam.hbase.entry.level2index;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;

/**
 * Description:  org.fasteam.aspect
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/10
 */
public class Level2IndexContext extends EnvironmentContext<Level2IndexProvider> {
    private final String INDEX_TYPE = "abilities.aspect.level2-index.type";
    private final String ES_INDEX_NODE = "abilities.aspect.level2-index.es.node";
    private final String ES_INDEX_PORT = "abilities.aspect.level2-index.es.port";
    private final String ES_INDEX_AUTH_USER = "abilities.aspect.level2-index.es.auth.username";
    private final String ES_INDEX_AUTH_PASS = "abilities.aspect.level2-index.es.auth.password";
    public static final String INDEX_KEY_COLUMN_NAME = "content"; //二级索引存储rowkey的那个列的列名
    public enum IndexType{
        elasticsearch;
        public boolean matches(String method) {
            return this.name().equalsIgnoreCase(method);
        }

    }
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String type = config.props.get(String.class,INDEX_TYPE);
        if(IndexType.elasticsearch.matches(type)){
            String node = config.props.get(String.class,ES_INDEX_NODE);
            String port = config.props.get(String.class,ES_INDEX_PORT);
            String user = config.props.get(String.class,ES_INDEX_AUTH_USER);
            String password = config.props.get(String.class,ES_INDEX_AUTH_PASS);
            setFeature(new EsLevel2IndexProvider(node,port,user,password));
        }

        super.initialization();
    }

    @Override
    public Level2IndexProvider callbackFeature() {
        return getFeature();
    }

    @Override
    public SparkSession.Builder buildSession(SparkSession.Builder sparkBuilder) {
        Level2IndexProvider provider = getFeature();
        if(provider.type().equals(IndexType.elasticsearch)){
            if(StringUtils.isNotBlank(((EsLevel2IndexProvider)provider).getNode())){
                return sparkBuilder.config("es.nodes",((EsLevel2IndexProvider)provider).getNode())
                        .config("es.port",((EsLevel2IndexProvider)provider).getPort())
                        .config("es.net.http.auth.user",((EsLevel2IndexProvider)provider).getUser())
                        .config("es.net.http.auth.pass",((EsLevel2IndexProvider)provider).getPass())
                        .config("es.index.auto.create","true");
            }
        }
        return super.buildSession(sparkBuilder);

    }
}

package org.fasteam.neo4j.entry;

import lombok.SneakyThrows;
import org.apache.spark.sql.DataFrameReader;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.conf.Config;

import static org.fasteam.neo4j.entry.Neo4jEnvironmentParameter.*;

/**
 * Description:  org.fasteam.neo4j.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/4
 */
public class Neo4jEnvironmentContext extends EnvironmentContext<Neo4jEnvironmentParameter> {
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String host = config.props.get(String.class,HOST);
        String user = config.props.get(String.class,USERNAME);
        String password = config.props.get(String.class,PASSWORD);
        setFeature(Neo4jEnvironmentParameter.builder().uri(host).username(user).password(password).build());
        super.initialization();
    }

    @Override
    public Neo4jEnvironmentParameter callbackFeature() {
        return getFeature();
    }

    public DataFrameReader getReader(){
        return SparkProcessor.getSession().read()
                .format("org.neo4j.spark.DataSource")
                .option("url", String.format("bolt://%s",getFeature().getUri()))
                .option("authentication.basic.username", getFeature().getUsername())
                .option("authentication.basic.password", getFeature().getPassword());
    }
}

package org.fasteam.mongo.entry;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;

import static org.fasteam.mongo.entry.MongoEnvironmentParameter.MONGO_HOST;
import static org.fasteam.mongo.entry.MongoEnvironmentParameter.MONGO_PORT;
import static org.fasteam.mongo.entry.MongoEnvironmentParameter.MONGO_USERNAME;
import static org.fasteam.mongo.entry.MongoEnvironmentParameter.MONGO_PASSWORD;
import static org.fasteam.mongo.entry.MongoEnvironmentParameter.MONGO_DATABASE;
import static org.fasteam.mongo.entry.MongoEnvironmentParameter.MONGO_DEFAULT_COLLECTION;

/**
 * Description:  Mongo的环境初始化
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public class MongoEnvironmentContext extends EnvironmentContext<MongoEnvironmentParameter> {
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String host = config.props.get(String.class,MONGO_HOST);
//        String port = config.props.get(String.class,MONGO_PORT);
        String user = config.props.get(String.class,MONGO_USERNAME);
        String password = config.props.get(String.class,MONGO_PASSWORD);
        String db = config.props.get(String.class,MONGO_DATABASE);
        String defaultCollection = config.props.get(String.class,MONGO_DEFAULT_COLLECTION);
        String currentUri=null;
        if(StringUtils.isBlank(user)||StringUtils.isBlank(password)){
            currentUri = String.format(MongoEnvironmentParameter.URL_FORMAT_NO_PWD,host,db,defaultCollection);
        }else{
            currentUri = String.format(MongoEnvironmentParameter.URL_FORMAT,user,password,host,db,defaultCollection);
        }
        setFeature(MongoEnvironmentParameter.builder().uri(currentUri).build());
        super.initialization();
    }

    @Override
    public MongoEnvironmentParameter callbackFeature() {
        return getFeature();
    }

    @Override
    public SparkSession.Builder buildSession(SparkSession.Builder sparkBuilder) {
        MongoEnvironmentParameter env = getFeature();
        return sparkBuilder.config("spark.mongodb.input.uri", env.getUri()).config("spark.mongodb.output.uri", env.getUri());
    }
}

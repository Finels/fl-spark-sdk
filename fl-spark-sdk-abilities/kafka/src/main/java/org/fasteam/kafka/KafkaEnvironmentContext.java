package org.fasteam.kafka;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;
import org.fasteam.sdk.core.shaded.com.google.common.Preconditions;
import org.fasteam.sdk.core.shaded.com.google.common.Strings;

import java.util.Properties;

/**
 * Description:  com.scs.bigdata.sdk.spark.context
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/26
 */
public class KafkaEnvironmentContext extends EnvironmentContext<Properties> {
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String bootstrap = config.props.get(String.class,KafkaBootstrap.KAFKA_BOOTSTRAP);
        String keySerializer = config.props.get(String.class,KafkaBootstrap.KAFKA_KEY_SERIALIZER);
        String valueSerializer = config.props.get(String.class,KafkaBootstrap.KAFKA_VALUE_SERIALIZER);
        String ack = config.props.get(String.class,KafkaBootstrap.KAFKA_ACK_MODE);
        //check
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bootstrap),String.format("\"%s\" property must not be null",KafkaBootstrap.KAFKA_BOOTSTRAP));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keySerializer),String.format("\"%s\" property must not be null",KafkaBootstrap.KAFKA_KEY_SERIALIZER));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(valueSerializer),String.format("\"%s\" property must not be null",KafkaBootstrap.KAFKA_VALUE_SERIALIZER));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(ack),String.format("\"%s\" property must not be null",KafkaBootstrap.KAFKA_ACK_MODE));
        Properties properties = new Properties();
        properties.put("bootstrap.servers",bootstrap);
        properties.put("key.serializer", keySerializer);
        properties.put("value.serializer",valueSerializer);
        properties.put("acks",ack);
        setFeature(properties);
        super.initialization();
    }

    @Override
    public Properties callbackFeature() {
        return getFeature();
    }

    public String getBootstrap(){
        return getFeature().getProperty("bootstrap.servers");
    }
}

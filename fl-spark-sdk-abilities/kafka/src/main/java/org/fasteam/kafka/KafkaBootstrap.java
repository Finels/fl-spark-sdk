package org.fasteam.kafka;

import lombok.Builder;
import lombok.Data;

/**
 * Description:  com.scs.bigdata.sdk.spark.context.parameter
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/26
 */
@Data
@Builder
public class KafkaBootstrap {
    public static final String KAFKA_BOOTSTRAP="abilities.kafka.bootstrap";
    public static final String KAFKA_KEY_SERIALIZER="abilities.kafka.key-serializer";
    public static final String KAFKA_VALUE_SERIALIZER="abilities.kafka.value-serializer";
    public static final String KAFKA_ACK_MODE="abilities.kafka.ack";
    private String bootstrap;

}

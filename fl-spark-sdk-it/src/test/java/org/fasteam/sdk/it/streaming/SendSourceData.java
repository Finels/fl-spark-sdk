package org.fasteam.sdk.it.streaming;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fasteam.kafka.KafkaEnvironmentContext;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;

import java.util.Properties;

/**
 * Description:  org.fasteam.sdk.it.streaming
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/17
 */
public class SendSourceData extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext){
        Properties kafkaProperties = ((KafkaEnvironmentContext) EnvironmentContextFactory.get(KafkaEnvironmentContext.class)).getFeature();
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties)) {
            JSONObject data = new JSONObject();
            data.put("value","1");
            data.put("name","james");
            data.put("age","666");
            producer.send(new ProducerRecord<>("kafka_test_ods", data.toJSONString()));
        }catch (Exception ignored){
            SparkProcessor.doThrow(ignored);
        }
    }
}

package org.fasteam.aspect;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.fasteam.kafka.KafkaEnvironmentContext;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;

import java.util.Properties;

/**
 * Description:  com.scs.bigdata.sdk.spark.listener
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/26
 */
public class FinalStateListener extends SparkListener {
    private String appId;
    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        super.onApplicationStart(applicationStart);
        this.appId = applicationStart.appId().get();
        System.out.println("==========APP_ID:"+appId);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        super.onApplicationEnd(applicationEnd);
        //任务结束后根据task执行状态发送最终状态结果到kafka
        if(FinalStateContext.getState()){
            Properties kafkaProperties = ((KafkaEnvironmentContext) EnvironmentContextFactory.get(KafkaEnvironmentContext.class)).getFeature();
            SdkControlParameter controlParameter = ((SdkControlContext) EnvironmentContextFactory.get(SdkControlContext.class)).getFeature();
            try(KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties)) {
                System.out.println("==========APP_ID:"+this.appId);
               producer.send(new ProducerRecord<>(controlParameter.getFinalStateTopic(),this.appId));
            }catch (Exception ignored){
                //todo 如果final状态发送失败，可能会造成业务方的回调失败，从而导致下游业务不会执行
                //如果上游spark任务的sink端是幂等设计（即重复运行这个spark任务也不会造成上游sink重复），那就可以忽略这个问题
                //因为这个异常可以抛出使得spark任务失败，重新再跑一次即可
                SparkProcessor.doThrow(new Exception("Final state listener send failed.",ignored));
            }
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        super.onTaskEnd(taskEnd);
        if(taskEnd.taskInfo().failed()){
            FinalStateContext.setState(false);
        }
    }
}

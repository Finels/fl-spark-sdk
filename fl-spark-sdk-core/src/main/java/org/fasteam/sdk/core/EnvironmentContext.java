package org.fasteam.sdk.core;

import org.apache.spark.sql.SparkSession;

/**
 * Description:  环境上下文的基础继承类
 * 用于加载通用环境变量
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public abstract class EnvironmentContext<T> {
   EnvironmentContext next;
   T feature;
    public void initialization(){
        if(next!=null) {
            next.initialization();
        }
    }
    public abstract T callbackFeature();

    //need to override
    public SparkSession.Builder buildSession(SparkSession.Builder sparkBuilder){
        return sparkBuilder;
    }

    public void setFeature(T feature) {
        this.feature = feature;
    }

    public T getFeature() {
        return feature;
    }

    void setNext(EnvironmentContext next){
      this.next=next;
   }
}

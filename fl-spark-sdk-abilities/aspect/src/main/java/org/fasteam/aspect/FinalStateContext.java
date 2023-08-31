package org.fasteam.aspect;

import org.apache.spark.sql.SparkSession;
import org.fasteam.sdk.core.EnvironmentContext;

/**
 * Description:  com.scs.bigdata.sdk.spark.context
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/26
 */
public class FinalStateContext extends EnvironmentContext<Void> {
    private static boolean state = true; //要么成功要么失败

    public synchronized static void setState(boolean state) {
        FinalStateContext.state = state;
    }
    public static boolean getState(){
        return FinalStateContext.state;
    }

    @Override
    public SparkSession.Builder buildSession(SparkSession.Builder sparkBuilder) {
        return sparkBuilder.config("spark.extraListeners","org.fasteam.aspect.FinalStateListener");
    }

    @Override
    public Void callbackFeature() {
        return null;
    }
}

package org.fasteam.aspect;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;

import static org.fasteam.aspect.SdkControlParameter.FINAL_STATE_TOPIC;

/**
 * Description:  com.scs.bigdata.sdk.spark.context
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/26
 */
public class SdkControlContext extends EnvironmentContext<SdkControlParameter> {
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String finalStateTopic = config.props.get(String.class,FINAL_STATE_TOPIC);
        setFeature(SdkControlParameter.builder().finalStateTopic(finalStateTopic).build());
        super.initialization();
    }

    @Override
    public SdkControlParameter callbackFeature() {
        return getFeature();
    }
}

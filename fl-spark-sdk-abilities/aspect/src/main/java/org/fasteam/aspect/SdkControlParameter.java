package org.fasteam.aspect;

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
public class SdkControlParameter {
    public static final String FINAL_STATE_TOPIC="abilities.aspect.final-state.topic-name";
    private String finalStateTopic;
}

package org.fasteam.sdk.core;

import lombok.AllArgsConstructor;

/**
 * Description:  SparkProcessor类中的一些硬编码
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
@AllArgsConstructor
public enum RuntimeEnum {
    ARGS_CLASS_NAME("runClass"),
    ARGS_APP_NAME("appName"),
    ARGS_SPARK_MASTER("master"),
//    ARGS_RUN_TYPE("runType"),
    ARGS_LOG_LEVEL("logLevel"),
    ARGS_USER_DEFINE("userDefineArgs"),
    CONF_LOCATE_TYPE("confLocate"),
    CONF_URI("confUri"),
    USER_ID("userId"),
    BATCH_ID("batchId"),
    ;


    private String name;

    public String getName() {
        return name;
    }
}

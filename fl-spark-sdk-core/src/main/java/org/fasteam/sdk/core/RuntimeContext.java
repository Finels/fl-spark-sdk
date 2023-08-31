package org.fasteam.sdk.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Description:  com.scs.bigdata.sdk.spark.config.constant
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/19
 */
public class RuntimeContext {
    private Map<RuntimeEnum,Object> bundle;

    protected void setBundle(String jsonArgs) {
        bundle = new HashMap<>();
        JSONObject argJsonObject = JSON.parseObject(jsonArgs);
        RuntimeEnum[] enums = RuntimeEnum.values();
        Arrays.stream(enums).forEach(argsEnum->{
            if(argJsonObject.get(argsEnum.getName())!=null) {
                bundle.put(argsEnum, argJsonObject.get(argsEnum.getName()));
            }
        });
    }
    public Object get(RuntimeEnum prefixEnum){
        return this.bundle.get(prefixEnum);
    }
}

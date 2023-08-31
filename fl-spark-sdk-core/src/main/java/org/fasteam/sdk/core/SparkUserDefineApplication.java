package org.fasteam.sdk.core;


import java.io.Serializable;

/**
 * Description:   用户自定义逻辑的实现接口，主要业务逻辑的实现接口
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public abstract class SparkUserDefineApplication implements Serializable {


    public abstract void process(RuntimeContext argsContext) throws Exception;

}

package org.fasteam.sdk.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.fasteam.sdk.core.conf.Config;
import org.fasteam.sdk.core.conf.ConfigProvider;
import org.fasteam.sdk.core.shaded.com.google.common.Preconditions;
import org.fasteam.sdk.core.shaded.com.google.common.Strings;

import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Base64;

/**
 * Description:  SparkSDK执行主类，程序入口类
 * 默认保存的es index名称为 ${INDEX_NAME}
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
@Slf4j
public class SparkProcessor implements Serializable {
    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;
    private static Broadcast<String> applicationId; //spark应用id
    private static Broadcast<String> userId;
    private static Broadcast<String> batchId;


    public static void main(String[] args) throws Exception {
        //===================获取args参数===================
            String arg = args[0];
        arg = new String(Base64.getDecoder().decode(arg));
        log.info("SDK got params:{}",arg);
        RuntimeContext argsContext = new RuntimeContext();
        argsContext.setBundle(arg);
        //设置运行时环境
        String confLocate = (String) argsContext.get(RuntimeEnum.CONF_LOCATE_TYPE);
        if(ConfigProvider.REMOTE.matches(confLocate)){
            String fileFromUri = (String) argsContext.get(RuntimeEnum.CONF_URI);
            Preconditions.checkArgument(!Strings.isNullOrEmpty((String) argsContext.get(RuntimeEnum.CONF_URI)),"\"confUri\" property cannot be null because the \"confLocateType\" is \"remote\"");
            //get config file from uri
            URL configFileUrl = new URL(fileFromUri);
            new Config(ConfigProvider.REMOTE.url(configFileUrl));
        }else if(ConfigProvider.LOCAL.matches(confLocate)){
            new Config(ConfigProvider.LOCAL);
        }else{
            throw new UnsupportedOperationException("\"confLocate\" property cannot be null.");
        }
        log.info("SDK loaded config succeed.");
//        System.setProperty(RuntimeEnum.ETL_PROFILE.getName(),(String) argsContext.get(RuntimeEnum.ETL_PROFILE));
//        System.setProperty(RuntimeEnum.NACOS_HTTP.getName(),(String) argsContext.get(RuntimeEnum.NACOS_HTTP));
        //region 初始化环境参数
        EnvironmentContextFactory.chain().initialization();
        //endregion
        //region 初始化spark context
        String defaultMaster = argsContext.get(RuntimeEnum.ARGS_SPARK_MASTER)==null?"yarn":(String) argsContext.get(RuntimeEnum.ARGS_SPARK_MASTER);
        SparkSession.Builder builder = SparkSession.builder()
                .appName((String) argsContext.get(RuntimeEnum.ARGS_APP_NAME))
                .master(defaultMaster)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.executor.extraJavaOptions","-Dfile.encoding=UTF-8")
                .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8");
        SparkSession spark =EnvironmentContextFactory.buildSession(builder).getOrCreate();
        SparkSession.clearDefaultSession();
        SparkContext context = spark.sparkContext();
        JavaSparkContext javaContext = JavaSparkContext.fromSparkContext(context);
        //log等级不为空则设置，否则不设置
        if (StringUtils.isNotBlank((String) argsContext.get(RuntimeEnum.ARGS_LOG_LEVEL))) {
            context.setLogLevel((String) argsContext.get(RuntimeEnum.ARGS_LOG_LEVEL));
        }
        //endregion

        //region 广播必要的环境变量
        javaSparkContext = javaContext;
        sparkSession = spark;
        applicationId = javaContext.broadcast(context.applicationId());
        userId = javaContext.broadcast((String) argsContext.get(RuntimeEnum.USER_ID));
        batchId = javaContext.broadcast((String) argsContext.get(RuntimeEnum.BATCH_ID));

        Class<?> aClass = Class.forName((String) argsContext.get(RuntimeEnum.ARGS_CLASS_NAME));
        SparkUserDefineApplication uda = (SparkUserDefineApplication) aClass.newInstance();
        uda.process(argsContext);
    }

    public static <E extends Exception> void doThrow(Exception e) throws E {
        throw (E) e;
    }
    public static JavaSparkContext getContext() {
        return javaSparkContext;
    }
    public static SparkSession getSession() {
        return sparkSession;
    }
    public static void stop() {
        getContext().stop();
    }
    public static String getApplicationId() {
        return applicationId.value();
    }

    public static String getBatchId() {
        return batchId.value();
    }

    public static String getUserId() {
        return userId.value();
    }
}

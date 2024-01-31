package org.fasteam.sdk.core;

import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;

import java.util.*;

/**
 * Description:  环境加载的工厂类，用于构建环境上下文调用链
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public class EnvironmentContextFactory {
    private static final LinkedHashMap<Class<? extends EnvironmentContext>,EnvironmentContext> contexts = new LinkedHashMap<>();

    static {
        Set<Class<? extends EnvironmentContext>> subCls = new Reflections("org.fasteam").getSubTypesOf(EnvironmentContext.class);
        subCls.forEach(subCl->{
            try {
                contexts.put(subCl,subCl.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
        List<EnvironmentContext> lst = new ArrayList<>(contexts.values());
        for (int i = 0; i < lst.size(); i++) {
            if(i+1<lst.size()) {
                lst.get(i).setNext(lst.get(i + 1));
            }
        }
    }
    public static EnvironmentContext chain() {
        //返回调用链头
        return contexts.values().stream().findFirst().orElse(null);
    }

    public static EnvironmentContext get(Class<? extends EnvironmentContext> className){
        return contexts.get(className);
    }

    public static SparkSession.Builder buildSession(SparkSession.Builder sparkBuilder){
        for (EnvironmentContext context : contexts.values()) {
            sparkBuilder = context.buildSession(sparkBuilder);
        }
        return sparkBuilder;
    }


    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        EnvironmentContextFactory.chain().initialization();
    }
}

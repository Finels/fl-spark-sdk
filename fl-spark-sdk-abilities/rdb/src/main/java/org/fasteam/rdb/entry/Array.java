package org.fasteam.rdb.entry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Description:  com.scs.bigdata.sdk.spark.annotation
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/4/23
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Array {
    Class<?> elementType();
}

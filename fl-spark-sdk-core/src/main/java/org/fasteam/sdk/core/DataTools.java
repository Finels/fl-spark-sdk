package org.fasteam.sdk.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:对hbase数据栏位进行校验的工具类
 * <p>
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public class DataTools {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static ObjectNode newNode() {
        return JSON.createObjectNode();
    }

    public static byte[] toBytes(ObjectNode node) {
        return toString(node).getBytes(StandardCharsets.UTF_8);
    }

    public static JsonNode fromString(String data) {
        try {
            return JSON.readTree(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toString(JsonNode node) {
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toObjString(Object node) {
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static StructType fromJavaBean(Class beanClass) {
        List<StructField> lst = new ArrayList<>();
        Field[] beanFields = beanClass.getDeclaredFields();
        for (Field beanField : beanFields) {
            lst.add(DataTypes.createStructField(beanField.getName(), changeDataType(beanField.getClass()), true));
        }
        return DataTypes.createStructType(lst.toArray(new StructField[]{}));
    }

    public static void main(String[] args) {
        System.out.println();
    }

    public static DataType changeDataType(Class typeClass) {
        String typeName = typeClass.getSimpleName();
        if (Integer.class.getSimpleName().equals(typeName)) {
            return DataTypes.IntegerType;
        } else if (String.class.getSimpleName().equals(typeName)) {
            return DataTypes.StringType;
        } else if (Long.class.getSimpleName().equals(typeName)) {
            return DataTypes.LongType;
        } else if (Double.class.getSimpleName().equals(typeName)) {
            return DataTypes.DoubleType;
        } else {
            return DataTypes.StringType;
        }
    }

    /**
     * 检查数据类型是否正确
     *
     * @param value 检验的数据
     * @param type  检验的类型
     **/
    public static boolean checkDataType(String value, String type) {
        switch (type) {
            case "INT":
                return value.equals(String.valueOf(Integer.parseInt(value)));
            case "LONG":
                return value.equals(String.valueOf(Long.parseLong(value)));
            case "STRING":
                return true;
            case "FLOAT":
                String values = value.replace("f", "").replace("F", "");
                if (values.length() > values.indexOf(".") + 2) {
                    values = clearEndStr(values, "0");
                    values = StringUtils.endsWith(values, ".") ? values + "0" : values;
                }
                return values.equals(String.valueOf(Float.parseFloat(value)));
            case "DOUBLE":
                values = value.replace("d", "").replace("D", "");
                if (values.length() > values.indexOf(".") + 2) {
                    values = clearEndStr(values, "0");
                    values = StringUtils.endsWith(values, ".") ? values + "0" : values;
                }
                return values.equals(String.valueOf(Double.parseDouble(value)));
            default:
                return false;
        }

    }

    /**
     * 清理后缀
     **/
    private static String clearEndStr(String value, String str) {
        if (StringUtils.endsWith(value, str)) {
            value = StringUtils.removeEnd(value, str);
            return clearEndStr(value, str);
        } else {
            return value;
        }
    }

    /**
     * @根据db的配置获取都应的spark ds中的类型配置
     **/
    public static DataType changeDataType(String configdt) {
        switch (configdt) {
            case "STRING":
                return DataTypes.StringType;
            case "INT":
                return DataTypes.IntegerType;
            case "LONG":
                return DataTypes.LongType;
            case "FLOAT":
                return DataTypes.FloatType;
            case "DOUBLE":
                return DataTypes.DoubleType;
            default:
                return DataTypes.StringType;
        }
    }

}

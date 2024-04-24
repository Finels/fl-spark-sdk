package org.fasteam.rdb.entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:  Mysql操作类
 * 默认保存的es index名称为 ${INDEX_NAME}
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
@Slf4j
public class Mysql {

    private static final String DB_TYPE = "mysql";

    /**
     * 根据SQL查询RDB中的数据，RDB由dbCode指定，需要预先配置dbCode
     * dbCode配置由NACOS中的[RDB_CONNECTION_PREFIX]配置项对应的DB配置表位置决定
     *
     * @param dbCode 指定RDB的代码
     * @param query  查询语句
     * @return  Row类型的Dataset
     **/
    public static Dataset<Row> sqlRead(String dbCode, String query) throws Exception {
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        return sqlRead(env.getUrl(), env.getUser(), env.getPass(), query);
    }
    /**
     * 根据表名查询RDB中的数据，RDB由dbCode指定，需要预先配置dbCode
     * dbCode配置由NACOS中的[RDB_CONNECTION_PREFIX]配置项对应的DB配置表位置决定
     * 直接查询指定表名的全部数据
     *
     * @param dbCode 指定RDB的代码
     * @param tableName 表名
     * @return  Row类型的Dataset
     **/
    public static Dataset<Row> tableRead(String dbCode, String tableName) throws Exception {
        String querySql = String.format("select * from %s",tableName);
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        return sqlRead(env.getUrl(), env.getUser(), env.getPass(), querySql);
    }

    /**
     * 读取mysql数据
     *
     * @param dbUrl      mysql 数据库连接：jdbc:mysql://dpmysql1:3306/KettleManager?useSSL=false&amp;allowMultiQueries=true;
     * @param dbUser     数据库账号
     * @param dbPassword 数据库密码
     * @param query      查询语句
     **/
    protected static Dataset<Row> sqlRead(String dbUrl, String dbUser, String dbPassword, String query) {
        SparkSession sparkSession = SparkProcessor.getSession();
        Dataset<Row> dataset = sparkSession.read().format("jdbc")
                .option("url", dbUrl)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", dbUser)
                .option("password", dbPassword)
                .option("query", query)
                .load();
        return dataset;
    }

    /***
     * 批量直接执行sql
     *
     * ***/
    protected static void commonOdbcExecuteSql(String dbUrl, String dbUser, String dbPassword, List<String> sqls) throws Exception {
        Driver mysqldriver = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
        Properties dbpro = new Properties();
        dbpro.put("user", dbUser);
        dbpro.put("password", dbPassword);
        try (Connection con = mysqldriver.connect(dbUrl, dbpro)) {
            con.setAutoCommit(true);
            Statement statement = con.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
//            con.commit();
        }
    }

    /**
     * 批量直接执行sql
     ***/
    public static void commonOdbcExecuteSql(String dbCode, List<String> sqls) throws Exception {
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        commonOdbcExecuteSql(env.getUrl(), env.getUser(), env.getPass(), sqls);
    }


    /**
     * 直接使用dataset写入，无法使用在海量数据
     **/
    protected static void commonDatasetWriteBatch(String dbUrl, String dbUser, String dbPassword, String tableName, Dataset<Row> insertData, SaveMode saveMode,Boolean isTruncate) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbUser);
        connectionProperties.put("password", dbPassword);
        connectionProperties.put("truncate", isTruncate.toString());
        insertData.write().mode(saveMode).jdbc(dbUrl, tableName, connectionProperties);
    }


    /**
     *
     * @param dbCode dbCode配置由NACOS中的[RDB_CONNECTION_PREFIX]配置项对应的DB配置表位置决定
     * @param tableName 目标db表名
     * @param insertData 待插入的dataset
     * @param saveMode 保存模式,APPEND或者OVERWRITE,注意 overwrite会覆盖掉原来目标表的表结构，一些索引和字段类型会被覆盖/删除
     * @throws Exception
     */
    public static void commonDatasetWriteBatch(String dbCode, String tableName, Dataset<Row> insertData, SaveMode saveMode) throws Exception {
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        commonDatasetWriteBatch(env.getUrl(), env.getUser(), env.getPass(), tableName, insertData, saveMode,false);
    }
    /**
     *
     * @param dbCode dbCode配置由NACOS中的[RDB_CONNECTION_PREFIX]配置项对应的DB配置表位置决定
     * @param tableName 目标db表名
     * @param insertData 待插入的dataset
     * @param saveMode 保存模式,APPEND或者OVERWRITE,注意 overwrite会覆盖掉原来目标表的表结构，一些索引和字段类型会被覆盖/删除
     * @param isTruncate 为true时，保存模式OVERWRITE时，是否清空目标表数据，不影响表结构
     * @throws Exception
     */
    public static void commonDatasetWriteBatch(String dbCode, String tableName, Dataset<Row> insertData, SaveMode saveMode,Boolean isTruncate) throws Exception {
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        commonDatasetWriteBatch(env.getUrl(), env.getUser(), env.getPass(), tableName, insertData, saveMode,isTruncate);
    }
}

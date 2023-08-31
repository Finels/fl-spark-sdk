package org.fasteam.rdb.entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:
 * <p> 读写sqlserver 数据库
 */
@Slf4j
public class SqlServer {

    private static final String DB_TYPE = "sqlserver";


    /**
     * 根据SQL查询RDB中的数据，RDB由dbCode指定，需要预先配置dbCode
     * dbCode配置由NACOS中的[RDB_CONNECTION_PREFIX]配置项对应的DB配置表位置决定
     *
     * @param dbCode 指定RDB的代码
     * @param query  查询语句
     * @return  Row类型的JavaRDD
     **/
    public static JavaRDD<Row> rddRead(String dbCode, String query) throws Exception {
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        return rddRead(env.getUrl(), env.getUser(), env.getPass(), query).toJavaRDD();
    }
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
        return rddRead(env.getUrl(), env.getUser(), env.getPass(), query);
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
        return sqlRead(dbCode, querySql);
    }



    /**
     * 读取sqlserver数据
     *
     * @param dbUrl      sqlserver 数据库连接：jdbc:sqlserver://127.0.0.1:1433;databaseName=DataLake;loginTimeout=90;
     * @param dbUser     数据库账号
     * @param dbPassword 数据库密码
     * @param query      查询语句
     **/
    protected static Dataset<Row> rddRead(String dbUrl, String dbUser, String dbPassword, String query) {
        SparkSession sparkSession = SparkProcessor.getSession();

        Dataset<Row> dataset = sparkSession.read().format("jdbc")
                .option("url", dbUrl)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
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
        Driver sqlserverDriver = (Driver) Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
        Properties dbpro = new Properties();
        dbpro.put("user", dbUser);
        dbpro.put("password", dbPassword);
        try (Connection con = sqlserverDriver.connect(dbUrl, dbpro)) {
            Statement statement = con.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            con.commit();
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
     * 直接使用dataset写入
     **/
    protected static void commonDatasetWriteBatch(String dbUrl, String dbUser, String dbPassword, String tablename,
                                                  Dataset<Row> dataset, SaveMode saveMode) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbUser);
        connectionProperties.put("password", dbPassword);
        dataset.write().mode(saveMode).jdbc(dbUrl, tablename, connectionProperties);
    }


    /**
     *
     * @param dbCode dbCode配置由NACOS中的[RDB_CONNECTION_PREFIX]配置项对应的DB配置表位置决定
     * @param tableName 目标db表名
     * @param dataset 待插入的dataset
     * @param saveMode 保存模式,APPEND或者OVERWRITE,注意 overwrite会覆盖掉原来目标表的表结构，一些索引和字段类型会被覆盖/删除
     * @throws Exception
     */
    public static void commonDatasetWriteBatch(String dbCode, String tableName, Dataset<Row> dataset, SaveMode saveMode) throws Exception {
        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        if (env == null) {
            throw new Exception("未找到数据库连接配置信息");
        }
        commonDatasetWriteBatch(env.getUrl(), env.getUser(), env.getPass(),
                tableName, dataset, saveMode);
    }
}

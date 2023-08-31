package org.fasteam.rdb.entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.fasteam.sdk.core.DataTools;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Description:  Postgres sql的操作类
 * 默认保存的es index名称为 ${INDEX_NAME}
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
@Slf4j
public class Postgresql {
    /**
     * 读取指定表的全部数据
     * @param database 读取的数据库名称
     * @param tableName 读取的表名，格式为[模式名.表名]，例如: public.sys_user
     * @return dataset对象
     */
    public static Dataset<Row> datasetRead(String code,String database, String tableName){

        RdbConnection env = ((RdbEnvironmentContext) EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(code);
        return SparkProcessor.getSession().read().format("jdbc")
                .option("driver","org.postgresql.Driver")
                .option("url",env.getUrl())
                .option("user",env.getUser())
                .option("password",env.getPass())
                .option("dbtable",tableName)
                .load();
    }
    public static int ddl(String code,String database,String sql) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        int update = 0;
        RdbConnection env = ((RdbEnvironmentContext)EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(code);
        Driver pgDriver = (Driver) Class.forName("org.postgresql.Driver").newInstance();
        Properties dbPro = new Properties();
        dbPro.put("user", env.getUser());
        dbPro.put("password", env.getPass());
        try (Connection con = pgDriver.connect(env.getUrl(), dbPro)) {
            Statement statement = con.createStatement();
            update = statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return update;
    }

    public static void batchDdl(String code,String database, JavaRDD<String> sqlRdd) {
        RdbConnection env = ((RdbEnvironmentContext)EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(code);
        sqlRdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> iterator) throws Exception {
                Driver pgDriver = (Driver) Class.forName("org.postgresql.Driver").newInstance();
                Properties dbPro = new Properties();
                dbPro.put("user", env.getUser());
                dbPro.put("password", env.getPass());
                try (Connection con = pgDriver.connect(env.getUrl(), dbPro)) {
                    con.setAutoCommit(false);
                    Integer sqlCounter = 0;
                    Statement statement = con.createStatement();
                    while (iterator.hasNext()){
                        String sql = iterator.next();
                        statement.addBatch(sql);
                        sqlCounter+=1;
                        if(sqlCounter>5000){
                            //5K的数据量提交一次
                            statement.executeBatch();
                            con.commit();
                            statement = con.createStatement();
                        }
                    }
                    statement.executeBatch();
                    con.commit();
                } catch (SQLException e) {
                    log.error("PGSQL: batch execute sql error.",e);
                    throw new RuntimeException(e);
                }
            }
        });

    }

    public static void datasetWrite(String code,Dataset<Row> ds, String database,String tableName, SaveMode saveMode){
        RdbConnection env = ((RdbEnvironmentContext)EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(code);
        ds.write().mode(saveMode).format("jdbc")
                .option("driver","org.postgresql.Driver")
                .option("url",env.getUrl())
                .option("user",env.getUser())
                .option("password",env.getPass())
                .option("truncate",true)
                .option("stringtype", "unspecified")
                .option("dbtable",tableName)
                .save();
    }

    /**
     * 执行sql并返回结果集，只能查询，谓词下推到db中，因此会比{@link #datasetRead(String, String, String)}方法更好
     * @param database 读取的数据库名称
     * @param sql 执行该sql，并返回sql的结果集
     * @return dataset对象
     */
    public static Dataset<Row> sqlRead(String code,String database,String sql){
        RdbConnection env = ((RdbEnvironmentContext)EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(code);
        return SparkProcessor.getSession().read().format("jdbc")
                .option("driver","org.postgresql.Driver")
                .option("url",env.getUrl())
                .option("user",env.getUser())
                .option("password",env.getPass())
                .option("query",sql)
                .load();
    }

    /**
     * 在{@link #sqlRead(String, String, String)}的基础上，增加了转换为指定bean class的逻辑
     * 会返回类型为bean class的JavaRDD
     * @param database 读取的数据库名称
     * @param sql 执行该sql，并返回sql的结果集
     * @return dataset对象
     */
    public static Dataset<Row> sqlRead(String code, String database, String sql,Class<?> beanClass){
        Dataset<Row> ds = sqlRead(code,database,sql);
        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {
            if(field.isAnnotationPresent(Array.class)){
                //校验一下return type是否为list类型
                if(field.getType() == List.class){
                    Array annotation = field.getAnnotation(Array.class);
                    List<StructField> structFields = typeRegression(annotation.elementType(),new ArrayList<>());
                    ArrayType schema = new ArrayType(DataTypes.createStructType(structFields),true);
                    ds = ds.withColumn(field.getName(), functions.from_json(ds.col(field.getName()),schema).as(field.getName()));
                }
            }else if(field.isAnnotationPresent(Struct.class)){
                List<StructField> structFields = typeRegression(field.getType(),new ArrayList<>());
                ds = ds.withColumn(field.getName(), functions.from_json(ds.col(field.getName()),DataTypes.createStructType(structFields)).as(field.getName()));
            }
        }
        return ds;
    }

    private static List<StructField> typeRegression(Class<?> colClass, List<StructField> structFields){
        Field[] fields =  colClass.getDeclaredFields();
        for (Field field : fields) {
            if(field.isAnnotationPresent(Array.class)){
                Array annotation = field.getAnnotation(Array.class);
                structFields.add(
                        DataTypes.createStructField(
                                field.getName(),
                                new ArrayType(
                                        DataTypes.createStructType(
                                                typeRegression(annotation.elementType(),new ArrayList<>())
                                        ),true),true));
            }else if(field.isAnnotationPresent(Struct.class)){
                structFields.add(DataTypes.createStructField(
                        field.getName(),
                        DataTypes.createStructType(typeRegression(field.getType(),new ArrayList<>())),true));
            }else {
                structFields.add(DataTypes.createStructField(field.getName(), DataTools.changeDataType(field.getType()),true));
            }
        }
        return structFields;
    }

}

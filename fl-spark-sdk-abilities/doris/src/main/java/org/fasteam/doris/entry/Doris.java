package org.fasteam.doris.entry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.SparkProcessor;

/**
 * Description:  PACKAGE_NAME
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/15
 */
public class Doris {
    public static Dataset<Row> read(String database,String table){
        DorisEnvironmentParameter context = ((DorisEnvironmentContext)EnvironmentContextFactory.get(DorisEnvironmentContext.class)).callbackFeature();
        return SparkProcessor.getSession().read().format("doris")
                .option("doris.fenodes",context.getUri())
                .option("user",context.getUsername())
                .option("password",context.getPassword())
                .option("doris.table.identifier",String.format("%s.%s",database,table))
                .load();
    }
    public static void write(Dataset<Row> ds,String database,String table,SaveMode saveMode){
        DorisEnvironmentParameter context = ((DorisEnvironmentContext)EnvironmentContextFactory.get(DorisEnvironmentContext.class)).callbackFeature();
        ds.write().format("doris")
                .option("doris.fenodes",context.getUri())
                .option("user",context.getUsername())
                .option("password",context.getPassword())
                .option("doris.table.identifier",String.format("%s.%s",database,table))
                .mode(saveMode)
                .save();
    }
    public static void write(Dataset<Row> ds,String[] columns,String database,String table,SaveMode saveMode){
        DorisEnvironmentParameter context = ((DorisEnvironmentContext)EnvironmentContextFactory.get(DorisEnvironmentContext.class)).callbackFeature();
        //将columns转换为逗号连接的字符串
        ds.write().format("doris")
                .option("doris.fenodes",context.getUri())
                .option("user",context.getUsername())
                .option("password",context.getPassword())
                .option("doris.table.identifier",String.format("%s.%s",database,table))
                .option("doris.write.fields",String.join(",",columns))
                .mode(saveMode)
                .save();
    }
}

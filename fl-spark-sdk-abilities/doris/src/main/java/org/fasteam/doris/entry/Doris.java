package org.fasteam.doris.entry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fasteam.sdk.core.EnvironmentContextFactory;

/**
 * Description:  PACKAGE_NAME
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/15
 */
public class Doris {
    public static void write(Dataset<Row> ds,String table){
        DorisEnvironmentParameter context = ((DorisEnvironmentContext)EnvironmentContextFactory.get(DorisEnvironmentContext.class)).callbackFeature();
        ds.write().format("doris")
                .option("doris.fenodes",context.getUri())
                .option("user",context.getUsername())
                .option("password",context.getPassword())
                .option("doris.table.identifier",table)
                .save();
    }
}

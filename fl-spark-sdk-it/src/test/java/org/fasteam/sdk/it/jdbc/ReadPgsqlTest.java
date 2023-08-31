package org.fasteam.sdk.it.jdbc;

import org.fasteam.rdb.entry.Postgresql;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkUserDefineApplication;

/**
 * Description:  org.fasteam.sdk.it.rdb
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/29
 */
public class ReadPgsqlTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Postgresql.sqlRead("14","improve_dev","select * from ods.scs_location").show();
    }
}

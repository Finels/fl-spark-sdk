package org.fasteam.sdk.it.neo4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fasteam.neo4j.entry.Neo4jEnvironmentContext;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkUserDefineApplication;

/**
 * Description:  org.fasteam.sdk.it.neo4j
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/4
 */
public class ReadTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Neo4jEnvironmentContext context = (Neo4jEnvironmentContext)EnvironmentContextFactory.get(Neo4jEnvironmentContext.class);
        Dataset<Row> ds = context.getReader().option("labels",":zsk").load();
        ds.show();
    }
}

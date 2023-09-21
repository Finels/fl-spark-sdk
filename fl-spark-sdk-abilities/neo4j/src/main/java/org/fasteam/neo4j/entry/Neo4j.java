package org.fasteam.neo4j.entry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.fasteam.sdk.core.EnvironmentContextFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Description:  org.fasteam.neo4j.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/5
 */
public class Neo4j {
    public Dataset<Row> read(String labels){
        Neo4jEnvironmentContext context = (Neo4jEnvironmentContext)EnvironmentContextFactory.get(Neo4jEnvironmentContext.class);
        return context.getReader().option("labels",labels).load();
    }
    public Dataset<Row> read(String cypherQuery, @Nullable String script){
        Neo4jEnvironmentContext context = (Neo4jEnvironmentContext)EnvironmentContextFactory.get(Neo4jEnvironmentContext.class);
        return context.getReader()
                .option("script",script)
                .option("query",cypherQuery).load();
    }
    public Dataset<Row> read(String relationship,@NotNull String source,@NotNull String target){
        Neo4jEnvironmentContext context = (Neo4jEnvironmentContext)EnvironmentContextFactory.get(Neo4jEnvironmentContext.class);
        return context.getReader()
                .option("relationship",relationship)
                .option("relationship.source.labels",source)
                .option("relationship.target.labels",target)
                .load();
    }
    public Dataset<Row> read(String relationship,@NotNull String source,@NotNull String target,Boolean nodeMapped){
        Neo4jEnvironmentContext context = (Neo4jEnvironmentContext)EnvironmentContextFactory.get(Neo4jEnvironmentContext.class);
        return context.getReader()
                .option("relationship.nodes.map", nodeMapped)
                .option("relationship",relationship)
                .option("relationship.source.labels",source)
                .option("relationship.target.labels",target)
                .load();
    }
    public void write(Dataset<Row> ds, SaveMode saveMode, @NotNull String query){

    }
    public void write(Dataset<Row> ds,SaveMode saveMode,@NotNull String labels,String nodeKeys){

    }
    public void write(Dataset<Row> ds,@NotNull String relationship,@NotNull String sourceLabels,@NotNull String targetLabels){

    }
    public void write(Dataset<Row> ds,SaveMode saveMode,@NotNull String relationship,@NotNull String sourceLabels,SaveMode sourceSaveMode,@NotNull String targetLabels,SaveMode targetSaveMode){

    }

}

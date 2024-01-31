package org.fasteam.sdk.it.neo4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.fasteam.neo4j.entry.Neo4j;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkUserDefineApplication;

/**
 * Description:  org.fasteam.sdk.it.neo4j
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/12/11
 */
public class EtlGraphLoadTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Neo4j neo4j = new Neo4j();
        Dataset<Row> ds = neo4j.read("" +
                "match(m1)-[p:etl{etlId:\"e3\"}]->(n1)where apoc.node.degree.out(n1)=0\n" +
                "with n1\n" +
                "match(m)-[p:etl{etlId:\"e3\"}]->(n)\n" +
                "call apoc.algo.dijkstraWithDefaultWeight(m, n1, 'etl','d',1) YIELD path as path,weight as weight \n" +
                "with m,n,n1,p,path,weight,relationships(path) AS rels\n" +
                "WHERE all(rel in rels WHERE rel.etlId = 'e3') \n" +
                "with apoc.node.degree.in(m) AS inDegree,\n" +
                "m.tableId as startTableId,m.type as startTableType,\n" +
                "n.tableId as endTableId,n.type as endTableType,\n" +
                "p.type as etlType,\n" +
                "p.etlId as etlId,\n" +
                "p.joinId as joinId,\n" +
                "p,length(path) as steps\n" +
                "order by steps desc\n" +
                "return *",null);
//        ds = ds.where("`rel.etlId` = 'e1'");
        ds.show();
        ds.where(ds.col("joinId").isNull()).withColumn("p",functions.array(ds.col("p"))).unionByName(
                ds.where(ds.col("joinId").isNotNull()).groupBy("joinId").agg(functions.collect_list("p").as("p"),
                        functions.min("startTableId").as("startTableId"),
                        functions.min("startTableType").as("startTableType"),
                        functions.min("endTableId").as("endTableId"),
                        functions.min("endTableType").as("endTableType"),
                        functions.min("etlType").as("etlType"),
                        functions.min("etlId").as("etlId"),
                        functions.max("steps").as("steps"),
                        functions.min("inDegree").as("inDegree"))
        ).show();
        //先处理入度为0的节点
        //再按steps的顺序从大到小依次处理
//        ds.printSchema();
//        ds.show();
    }
}

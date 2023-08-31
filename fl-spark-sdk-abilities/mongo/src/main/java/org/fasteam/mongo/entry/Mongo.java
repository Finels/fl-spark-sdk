package org.fasteam.mongo.entry;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.MongoConnector;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.fasteam.sdk.core.SparkProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * Description:  Mongo库的操作帮助类，可以读写mongo，也可以按条件删除mongo中的数据
 * 默认保存的es index名称为 ${INDEX_NAME}
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
public class Mongo {
    public static JavaMongoRDD<Document> rddRead(String collectionName){
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", collectionName);
        ReadConfig readConfig = ReadConfig.create(SparkProcessor.getContext()).withOptions(readOverrides);
        return MongoSpark.load(SparkProcessor.getContext(),readConfig);
    }

    public static Dataset datasetRead(String collectionName, Class<?> beanClass){
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", collectionName);
        readOverrides.put("readPreference.name", "primaryPreferred");
        ReadConfig readConfig = ReadConfig.create(SparkProcessor.getContext()).withOptions(readOverrides);
        return MongoSpark.load(SparkProcessor.getContext(),readConfig).toDS(beanClass);
    }

    public static void datasetWrite(Dataset collection, String collectionName, SaveMode saveMode){
        MongoSpark.write(collection).option("collection", collectionName).mode(saveMode).save();
    }

    public static void bulkDelete(String collectionName,Bson condition){
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", collectionName);
        writeOverrides.put("writePreference.name", "primaryPreferred");
        WriteConfig writeConfig = WriteConfig.create(SparkProcessor.getContext()).withOptions(writeOverrides);
        MongoConnector.create(SparkProcessor.getContext()).withCollectionDo(writeConfig, MongoBaseBean.class,new Function<MongoCollection<MongoBaseBean>, Document>() {
            @Override
            public Document call(MongoCollection<MongoBaseBean> mongoCollection) throws Exception {
                mongoCollection.deleteMany(condition);
                return null;
            }
        });
    }
    private void generateBson(){
        String[] a = new String[]{"1","2"};
        Bson b = Filters.in("_id",a);
    }
}

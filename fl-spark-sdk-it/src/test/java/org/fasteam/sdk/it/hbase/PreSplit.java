package org.fasteam.sdk.it.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.fasteam.hbase.entry.Hbase;
import org.fasteam.hbase.entry.HbaseEnvironmentContext;
import org.fasteam.sdk.core.EnvironmentContextFactory;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.hadoop.hbase.client.TableDescriptorBuilder.SPLIT_POLICY;

/**
 * Description:  org.fasteam.sdk.it.hbase
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/8
 */
public class PreSplit extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Configuration configuration = ((HbaseEnvironmentContext) EnvironmentContextFactory.get(HbaseEnvironmentContext.class)).getFeature().getConfiguration();
        TableName tableName = TableName.valueOf("hbase_assemble_test");
        try(Connection conn = ConnectionFactory.createConnection(configuration)){
            Admin admin = conn.getAdmin();
            if(admin.tableExists(tableName)){
                if(admin.isTableEnabled(tableName)){
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
            builder.setCompressionType(Compression.Algorithm.SNAPPY);
            builder.setBloomFilterType(BloomType.ROW);
            tableDescriptor.setColumnFamily(builder.build());
            //关闭自动分区
            tableDescriptor.setValue(SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());
            admin.createTable(tableDescriptor.build(),getSplitKeys());
        }
    }

    private byte[][] getSplitKeys(){
        List<byte[]> splitKeys = new ArrayList<>();
        String fixedKeys = "00|01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19";
        for (String s : fixedKeys.split("\\|")) {
            splitKeys.add(Bytes.toBytes(s));
        }
        return splitKeys.toArray(new byte[][]{});
    }
}

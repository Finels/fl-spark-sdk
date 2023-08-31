package org.fasteam.sdk.it;

import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.it.hbase.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;

/**
 * Description:  针对Hbase的单一大表测试
 * 测试内容：1.创建预分区表
 *         2.批量写入Hbase至100*1024*1024条数据。
 *         3.测试加盐读取某批数据
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/8
 */
@Suite
public class HbasePressureTest {
    @DisplayName("创建测试预分区表")
    @Test
    @Order(0)
    public void test0() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.hbase.PreSplit\",\"appName\":\"HbaseWriteTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
    @DisplayName("Hbase大批量写入测试")
    @Test
    @Order(1)
    public void test1() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.hbase.BulkWriteTest\",\"appName\":\"HbaseBulkWriteTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }

    @DisplayName("Hbase加盐读取速率测试")
    @Test
    @Order(2)
    public void test2() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.hbase.QuickSaltReadTest\",\"appName\":\"HbaseBulkWriteTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
}

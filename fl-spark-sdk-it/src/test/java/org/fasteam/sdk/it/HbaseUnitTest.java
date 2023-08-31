package org.fasteam.sdk.it;

import org.fasteam.sdk.core.SparkProcessor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import java.nio.charset.StandardCharsets;
import java.util.Base64;


/**
 * Description:  针对Hbase基本功能的单元测试
 * 测试内容：1.创建预分区表
 *         2.写入两条加盐数据
 *         3.非加盐读取与加盐读取
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/7/7
 */
@Suite
@SelectClasses({HbaseUnitTest.class})
public class HbaseUnitTest {
    @DisplayName("创建测试预分区表")
    @Test
    @Order(0)
    public void test0() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.hbase.PreSplit\",\"appName\":\"HbaseWriteTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }

    @DisplayName("Hbase写入测试")
    @Test
    @Order(1)
    public void test1() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.hbase.UnitWriteTest\",\"appName\":\"HbaseWriteTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }

    @DisplayName("Hbase读取测试")
    @Test
    @Order(2)
    public void test2() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.hbase.UnitReadTest\",\"appName\":\"HbaseReadTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
}

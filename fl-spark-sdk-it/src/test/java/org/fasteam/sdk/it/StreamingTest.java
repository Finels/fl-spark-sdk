package org.fasteam.sdk.it;

import org.fasteam.sdk.core.SparkProcessor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Description:  org.fasteam.sdk.it
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/21
 */
public class StreamingTest {
    @DisplayName("预创建Hbase表")
    @Test
    @Order(0)
    public void test() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.streaming.PreBuildHbaseTable\",\"appName\":\"PreBuildHbaseTable\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
    @DisplayName("发送测试消息到kafka")
    @Test
    @Order(1)
    public void test1() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.streaming.SendSourceData\",\"appName\":\"SendSourceData\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
    @DisplayName("订阅kafka并持续写入hbase和下一个kafka队列")
    @Test
    @Order(2)
    public void test2() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.streaming.Sink2Hbase\",\"appName\":\"Sink2Hbase\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
}

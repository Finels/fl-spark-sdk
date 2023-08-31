package org.fasteam.sdk.it;

import org.fasteam.sdk.core.SparkProcessor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Description:  org.fasteam.sdk.it
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/11
 */
public class Level2IndexTest {

    @DisplayName("二级索引写入测试")
    @Test
    public void test() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.level2index.WriteTest\",\"appName\":\"Level2IndexTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
    @DisplayName("二级索引读取测试")
    @Test
    public void test1() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.level2index.ReadTest\",\"appName\":\"Level2IndexTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
}

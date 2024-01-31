package org.fasteam.sdk.it;

import org.fasteam.sdk.core.SparkProcessor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Description:  org.fasteam.sdk.it
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/4
 */
public class Neo4jTest {
    @DisplayName("标签读取测试")
    @Test
    public void test1() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.neo4j.EtlGraphLoadTest\",\"appName\":\"ReadTest\",\"master\":\"local[*]\",\"confLocate\":\"local\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
}

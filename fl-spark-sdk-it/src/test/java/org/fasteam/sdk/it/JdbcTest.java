package org.fasteam.sdk.it;

import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Description:  org.fasteam.sdk.example
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/2
 */

public class JdbcTest {
    @DisplayName("pgsql读取测试")
    @Test
    public void test() throws Exception {
        String args = "{\"runClass\":\"org.fasteam.sdk.it.jdbc.ReadPgsqlTest\",\"appName\":\"ReadPgsqlTest\",\"master\":\"local[*]\",\"confLocate\":\"remote\",\"confUri\":\"http://127.0.0.1:32005/nacos/v1/cs/configs?dataId=fl-sprk-sdk-rules.xml&group=DEFAULT_GROUP\"}";
        String params = Base64.getEncoder().encodeToString(args.getBytes(StandardCharsets.UTF_8));
        SparkProcessor.main(new String[]{params});
    }
}

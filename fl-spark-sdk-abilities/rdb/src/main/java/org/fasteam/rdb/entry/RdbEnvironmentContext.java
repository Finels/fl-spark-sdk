package org.fasteam.rdb.entry;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;
import org.fasteam.sdk.core.shaded.com.google.common.Preconditions;
import org.fasteam.sdk.core.shaded.com.google.common.Strings;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.fasteam.rdb.entry.RdbEnvironmentParameter.RDB_CONFIG_URL;
import static org.fasteam.rdb.entry.RdbEnvironmentParameter.RDB_CONFIG_STORE_TYPE_QUERY_SQL;
import static org.fasteam.rdb.entry.RdbEnvironmentParameter.RDB_CONFIG_USER;
import static org.fasteam.rdb.entry.RdbEnvironmentParameter.RDB_CONFIG_PASS;
import static org.fasteam.rdb.entry.RdbEnvironmentParameter.RDB_CONFIG_STORE_TYPE;

/**
 * Description:  RDB连接器的环境初始化
 *
 * @author FL
 * @version 1.0
 * @timestamp 2021/5/7
 */
@Slf4j
public class RdbEnvironmentContext extends EnvironmentContext<RdbEnvironmentParameter> {

    @Override
    public void initialization() {
        try {
            Config config = Config.getInstance();
            String storeType = config.props.get(String.class, RDB_CONFIG_STORE_TYPE);
            if (RdbEnvironmentStore.EXTERNAL.matches(storeType)) {
                String jdbcUrl = config.props.get(String.class, RDB_CONFIG_URL);
                String confQuerySql = config.props.get(String.class, RDB_CONFIG_STORE_TYPE_QUERY_SQL);
                String username = config.props.get(String.class, RDB_CONFIG_USER);
                String password = config.props.get(String.class, RDB_CONFIG_PASS);
                Preconditions.checkArgument(!Strings.isNullOrEmpty(jdbcUrl), "\"sdk.rdb.config.url\" property must not be null");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(confQuerySql), "\"sdk.rdb.config.query.sql\" property must not be null");

                List<RdbConnection> connLst = new ArrayList<>();
                //JDBC单连接去获取配置表信息
                Driver mysqldriver = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
                Properties dbParam = new Properties();
                dbParam.put("user", username);
                dbParam.put("password", password);
                try (Connection conn = mysqldriver.connect(jdbcUrl, dbParam)) {
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(confQuerySql);
                    ResultSetMetaData metaData = rs.getMetaData();
                    int totalColCount = metaData.getColumnCount();
                    while (rs.next()) {
                        JSONObject row = new JSONObject();
                        for (int i = 1; i <= totalColCount; i++) {
                            String columnName = metaData.getColumnLabel(i);
                            String value = rs.getString(columnName);
                            row.put(columnName, value);
                        }
                        connLst.add(row.toJavaObject(RdbConnection.class));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Unexpected error occurred while querying rdb config table.", e);
                }
                setFeature(RdbEnvironmentParameter.builder().rdbConnections(connLst).build());
            } else if (RdbEnvironmentStore.INTERNAL.matches(storeType)) {
                //TODO
                throw new UnsupportedOperationException();
            }
        }catch (Exception e){
            //ignore
            log.warn("{} initialize failed.",RdbEnvironmentContext.class.getName());
        }
        super.initialization();
    }

    @Override
    public RdbEnvironmentParameter callbackFeature() {
        return getFeature();
    }

    public RdbConnection getRdbConnection(String code){
       return this.callbackFeature().getRdbConnections().stream().filter(r->r.getId().equals(code)).findFirst().orElse(null);
    }
}

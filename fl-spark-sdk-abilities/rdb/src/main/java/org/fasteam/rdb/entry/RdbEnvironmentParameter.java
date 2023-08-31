package org.fasteam.rdb.entry;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class RdbEnvironmentParameter {
    public static final String RDB_CONFIG_STORE_TYPE="abilities.rdb.config.type";
    public static final String RDB_CONFIG_URL="abilities.rdb.config.url";
    public static final String RDB_CONFIG_USER="abilities.rdb.config.user";
    public static final String RDB_CONFIG_PASS="abilities.rdb.config.pass";
    public static final String RDB_CONFIG_STORE_TYPE_QUERY_SQL="abilities.rdb.config.query-sql";

    private List<RdbConnection> rdbConnections;
}

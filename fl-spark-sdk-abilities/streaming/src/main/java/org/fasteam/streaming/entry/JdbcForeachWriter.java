package org.fasteam.streaming.entry;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.fasteam.rdb.entry.RdbConnection;
import org.fasteam.rdb.entry.RdbEnvironmentContext;
import org.fasteam.rdb.entry.SqlTranslator;
import org.fasteam.sdk.core.EnvironmentContextFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Description:  org.fasteam.streaming.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/15
 */
@Slf4j
public class JdbcForeachWriter extends ForeachWriter<Row> {
    private Connection connection;
    private Statement statement;
    private RdbConnection rdbConnection;
    private String dbCode;
    private String targetTableName;

    public JdbcForeachWriter(String dbCode, String targetTableName) {
        this.dbCode = dbCode;
        this.targetTableName = targetTableName;
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        rdbConnection = ((RdbEnvironmentContext)EnvironmentContextFactory.get(RdbEnvironmentContext.class)).getRdbConnection(dbCode);
        try {
            connection = DriverManager.getConnection(rdbConnection.getUrl());
            statement = connection.createStatement();
        }catch (SQLException e){
            log.error("JdbcWriter open failed.",e);
            return false;
        }
        return true;
    }

    @SneakyThrows
    @Override
    public void process(Row value) {
        String sql = (rdbConnection.getDbType().getSqlTranslator().newInstance()).call(value,targetTableName);
        statement.executeUpdate(sql);
    }

    @SneakyThrows
    @Override
    public void close(Throwable errorOrNull) {
        connection.close();
    }
}

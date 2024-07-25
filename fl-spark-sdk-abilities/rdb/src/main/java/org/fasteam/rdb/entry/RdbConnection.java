package org.fasteam.rdb.entry;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName RdbConnection
 * @Description: 关系型数据库连接配置
 * @Author FL
 * @Date 2019/12/14
 * @Version V1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RdbConnection implements Serializable {
    private String id;
    private String name;
    private String status;
    private String dbType;
    private String ip;
    private String port;
    private String dbName;
    @JSONField(name = "user_name")
    private String user;
    @JSONField(name = "password")
    private String pass;
    private String url;
    public enum DbType{
        MYSQL("jdbc:mysql://%s:%s/%s?useSSL=false&allowMultiQueries=true&autoReconnect=true", MysqlTranslator.class),
        SQLSERVER("jdbc:sqlserver://%s:%s;databaseName=%s;",SqlserverTranslator.class),
        PGSQL("jdbc:postgresql://%s:%s/%s",PgsqlTranslator.class);
        private final String format;
        private final Class<? extends SqlTranslator> sqlTranslator;

        public String getFormat() {
            return format;
        }

        public Class<? extends SqlTranslator> getSqlTranslator() {
            return sqlTranslator;
        }

        DbType(String format, Class<? extends SqlTranslator> sqlTranslator) {
            this.format = format;
            this.sqlTranslator = sqlTranslator;
        }

        private static final Map<String, DbType> mappings = new HashMap<>(16);
        static {
            DbType[] var0 = values();
            int var1 = var0.length;
            for(int var2 = 0; var2 < var1; ++var2) {
                DbType callbackEnum = var0[var2];
                mappings.put(callbackEnum.name(), callbackEnum);
                mappings.put(callbackEnum.name().toLowerCase(),callbackEnum);
            }
        }
        @Nullable
        public static DbType resolve(@Nullable String method) {
            return method != null ? mappings.get(method) : null;
        }
        public boolean matches(String method) {
            return this.name().equalsIgnoreCase(method);
        }
    }

    @Override
    public String toString() {
        return "RdbConnection{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", status='" + status + '\'' +
                ", dbType='" + dbType + '\'' +
                ", ip='" + ip + '\'' +
                ", port='" + port + '\'' +
                ", dbName='" + dbName + '\'' +
                ", user='" + user + '\'' +
                ", pass='" + pass + '\'' +
                '}';
    }

    public DbType getDbType() {
        return DbType.resolve(this.dbType);
    }
    public String getUrl(){
        return String.format(this.getDbType().getFormat(),ip,port,dbName);
    }
}

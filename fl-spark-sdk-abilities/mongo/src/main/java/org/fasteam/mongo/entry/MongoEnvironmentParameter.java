package org.fasteam.mongo.entry;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MongoEnvironmentParameter {
    public static final String MONGO_HOST="abilities.mongo.host";
    public static final String MONGO_PORT="abilities.mongo.port";
    public static final String MONGO_USERNAME="abilities.mongo.user";
    public static final String MONGO_PASSWORD="abilities.mongo.pass";
    public static final String MONGO_DATABASE="abilities.mongo.database";
    public static final String MONGO_DEFAULT_COLLECTION="abilities.mongo.default-collection";
    //user, pwd , ip:port , dataBase,defaultCollection
    public static final String URL_FORMAT="mongodb://%s:%s@%s/%s.%s";
    //ip:port , dataBase,defaultCollection
    public static final String URL_FORMAT_NO_PWD="mongodb://%s/%s.%s";
    private String uri;
}

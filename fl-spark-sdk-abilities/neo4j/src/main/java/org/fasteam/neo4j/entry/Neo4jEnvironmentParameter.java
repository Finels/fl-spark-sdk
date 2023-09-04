package org.fasteam.neo4j.entry;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Neo4jEnvironmentParameter {
    public static final String HOST="abilities.neo4j.host";
    public static final String PORT="abilities.neo4j.port";
    public static final String USERNAME="abilities.neo4j.user";
    public static final String PASSWORD="abilities.neo4j.pass";
    private String uri;
    private String username;
    private String password;
}

package org.fasteam.doris.entry;

import lombok.Builder;
import lombok.Data;

/**
 * Description:  PACKAGE_NAME
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/15
 */
@Builder
@Data
public class DorisEnvironmentParameter {
    public static final String HOST="abilities.doris.fenodes";
    public static final String USERNAME="abilities.doris.user";
    public static final String PASSWORD="abilities.doris.pass";
    private String uri;
    private String username;
    private String password;
}

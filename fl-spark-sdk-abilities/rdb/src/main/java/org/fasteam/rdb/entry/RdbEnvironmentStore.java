package org.fasteam.rdb.entry;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Description:  org.fasteam.rdb.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/6/28
 */
public enum RdbEnvironmentStore {
    EXTERNAL,INTERNAL;

    private static final Map<String, RdbEnvironmentStore> mappings = new HashMap<>(16);

    private RdbEnvironmentStore() {
    }

    @Nullable
    public static RdbEnvironmentStore resolve(@Nullable String method) {
        return method != null ? mappings.get(method) : null;
    }

    public boolean matches(String method) {
        return this.name().equalsIgnoreCase(method);
    }

    static {
        RdbEnvironmentStore[] var0 = values();
        int var1 = var0.length;
        for(int var2 = 0; var2 < var1; ++var2) {
            RdbEnvironmentStore callbackEnum = var0[var2];
            mappings.put(callbackEnum.name(), callbackEnum);
        }
    }
}

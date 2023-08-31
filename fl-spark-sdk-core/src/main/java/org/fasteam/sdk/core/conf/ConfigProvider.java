package org.fasteam.sdk.core.conf;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Description:  org.fasteam.sdk.core.conf
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/6/27
 */
public enum ConfigProvider {

    REMOTE,LOCAL;

    private URL url;

    private static final Map<String, ConfigProvider> mappings = new HashMap<>(16);

    private ConfigProvider() {
    }

    ConfigProvider(URL url) {
        this.url = url;
    }

    public URL getUrl() {
        return url;
    }

    public ConfigProvider url(URL url) {
        this.url = url;
        return this;
    }

    @Nullable
    public static ConfigProvider resolve(@Nullable String method) {
        return method != null ? mappings.get(method) : null;
    }

    public boolean matches(String method) {
        return this.name().equalsIgnoreCase(method);
    }

    static {
        ConfigProvider[] var0 = values();
        int var1 = var0.length;
        for(int var2 = 0; var2 < var1; ++var2) {
            ConfigProvider configProvider = var0[var2];
            mappings.put(configProvider.name(), configProvider);
        }
    }
}

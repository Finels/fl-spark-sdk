package org.fasteam.hbase.entry.effkey;

import org.fasteam.hbase.entry.HbaseEnvironmentParameter;
import org.fasteam.sdk.core.conf.Config;

import javax.management.InstanceNotFoundException;
import java.io.IOException;

/**
 * Description:  org.fasteam.hbase.entry.effkey
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/7/25
 */
public class EffKeyProvider extends KeyProvider {
    private ProviderSchema providerSchema;
    private EffKeyLock keyLock;

    public enum ProviderSchema{
        HBASE_DICT,
        ES_DICT
    }
    public static EffKeyProvider getInstance(ProviderSchema schema,EffKeyLock keyLock){
        return new EffKeyProvider(schema,keyLock);
    }
    public static EffKeyProvider getInstance(ProviderSchema schema) throws IOException, InstanceNotFoundException {
        Config config = Config.getInstance();
        String zkQuorum = config.props.get(String.class, HbaseEnvironmentParameter.ZOOKEEPER_STRING);
        return new EffKeyProvider(schema,new EffKeyLock(5000, () -> zkQuorum));
    }
    private EffKeyProvider(ProviderSchema schema,EffKeyLock keyLock){
        this.providerSchema = schema;
        this.keyLock = keyLock;
    }

    @Override
    KeyDict provide() {
        switch (this.providerSchema){
            case HBASE_DICT:{
                return new HbaseKeyDict(this.keyLock);
            }
            case ES_DICT:{
                throw new UnsupportedOperationException();
            }
            default:{
                throw new IllegalArgumentException();
            }
        }
    }
}

package org.fasteam.hbase.entry.effkey;

/**
 * Description:  org.fasteam.hbase.entry.effkey
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/7/25
 */
public abstract class KeyProvider {
    abstract KeyDict provide();

    public KeyProvider() {

    }
}

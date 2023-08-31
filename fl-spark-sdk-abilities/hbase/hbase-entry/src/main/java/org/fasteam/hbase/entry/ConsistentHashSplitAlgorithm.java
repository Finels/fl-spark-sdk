package org.fasteam.hbase.entry;

import org.apache.hadoop.hbase.util.RegionSplitter;

/**
 * Description:  org.fasteam.hbase.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/8
 */
public class ConsistentHashSplitAlgorithm implements RegionSplitter.SplitAlgorithm {
    @Override
    public byte[] split(byte[] bytes, byte[] bytes1) {
        return new byte[0];
    }

    @Override
    public byte[][] split(int i) {
        return new byte[0][];
    }

    @Override
    public byte[][] split(byte[] bytes, byte[] bytes1, int i, boolean b) {
        return new byte[0][];
    }

    @Override
    public byte[] firstRow() {
        return new byte[0];
    }

    @Override
    public byte[] lastRow() {
        return new byte[0];
    }

    @Override
    public void setFirstRow(String s) {

    }

    @Override
    public void setLastRow(String s) {

    }

    @Override
    public byte[] strToRow(String s) {
        return new byte[0];
    }

    @Override
    public String rowToStr(byte[] bytes) {
        return null;
    }

    @Override
    public String separator() {
        return null;
    }

    @Override
    public void setFirstRow(byte[] bytes) {

    }

    @Override
    public void setLastRow(byte[] bytes) {

    }
}

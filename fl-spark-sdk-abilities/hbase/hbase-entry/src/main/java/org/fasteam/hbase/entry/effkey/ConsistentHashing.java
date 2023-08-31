package org.fasteam.hbase.entry.effkey;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class is a specific implementation of the consistent hashing algorithm.
 * It is used to evenly distribute a fixed-length "content" string
 * across a hash ring and returns an integer "salt" value.
 */
public class ConsistentHashing implements Serializable {

    private TreeMap<Long, String> virtualNodes = new TreeMap<>();
    private LinkedList<String> nodes;
    private int replicCnt;
    private final int nodeCount = 20; //默认分区数

    public ConsistentHashing(Integer... nodecounts) {
        this.nodes = null;
        this.replicCnt = 500;
        Integer nodecount = nodecounts.length!=0?nodecounts[0].intValue():20;
        init(nodecount);
    }

    /**
     * 初始化哈希环
     * 循环计算每个node名称的哈希值，将其放入treeMap
     */
    private void init(Integer nodecounts) {
        if (this.nodes == null) {
            nodes = new LinkedList<>();
            int nodecount = nodecounts==null ? 20:nodecounts;
            for (int i = 0; i < nodecount; i++) {
                //添加所有的节点信息名称 如 00 01  ....... 19
                if (i < 10) {
                    nodes.add("0" + String.valueOf(i));
                } else {
                    nodes.add(String.valueOf(i));
                }
            }
        }

        for (String nodeName : nodes) {
            for (int i = 0; i < replicCnt / 4; i++) {
                String virtualNodeName = getNodeNameByIndex(nodeName, i);
                for (int j = 0; j < 4; j++) {
                    virtualNodes.put(hash(virtualNodeName, j), nodeName);
                }
            }
        }
    }

    private String getNodeNameByIndex(String nodeName, int index) {
        return new StringBuffer(nodeName)
                .append("&&")
                .append(index)
                .toString();
    }

    /**
     * 根据传入的关键字返回对应的rowkey盐的信息
     *
     * @param key
     * @return 节点名称
     */
    public String selectNode(String key) {
        Long hashOfKey = hash(key, 0);
        if (!virtualNodes.containsKey(hashOfKey)) {
            Map.Entry<Long, String> entry = virtualNodes.ceilingEntry(hashOfKey);
            if (entry != null) {
                {
                    return entry.getValue();
                }
            }
            else {
                return nodes.getFirst();
            }
        } else {
            return virtualNodes.get(hashOfKey);
        }
    }

    private Long hash(String nodeName, int number) {
        byte[] digest = md5(nodeName);
        return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                | (digest[number * 4] & 0xFF))
                & 0xFFFFFFFFL;
    }

    /**
     * md5加密
     *
     * @param str
     * @return
     */
    private byte[] md5(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(str.getBytes("UTF-8"));
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 添加节点
     *
     * @param node
     * @return
     */
    private void addNode(String node) {
        nodes.add(node);
        String virtualNodeName = getNodeNameByIndex(node, 0);
        for (int i = 0; i < replicCnt / 4; i++) {
            for (int j = 0; j < 4; j++) {
                virtualNodes.put(hash(virtualNodeName, j), node);
            }
        }
    }

    /**
     * 移除不需要的节点
     *
     * @param node
     * @return
     */
    private void removeNode(String node) {
        nodes.remove(node);
        String virtualNodeName = getNodeNameByIndex(node, 0);
        for (int i = 0; i < replicCnt / 4; i++) {
            for (int j = 0; j < 4; j++) {
                virtualNodes.remove(hash(virtualNodeName, j), node);
            }
        }
    }

    /**
     * 打印所有节点
     */
    private void printTreeNode() {
        if (virtualNodes != null && !virtualNodes.isEmpty()) {
            virtualNodes.forEach((hashKey, node) ->
                    System.out.println(
                            new StringBuffer(node)
                                    .append(" ==> ")
                                    .append(hashKey)
                    )
            );
        } else {
            System.out.println("Cycle is Empty");
        }
    }
}

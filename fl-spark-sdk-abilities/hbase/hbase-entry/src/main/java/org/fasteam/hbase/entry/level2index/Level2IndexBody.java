package org.fasteam.hbase.entry.level2index;

import lombok.Data;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Description:  org.fasteam.aspect
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/8/10
 */
@Data
public class Level2IndexBody {
    private String indexColumn;
    private String columnValue;
    private String searchKey;
}

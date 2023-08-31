package org.fasteam.hbase.entry.log;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @program: fl-spark-sdk
 * @description: HbaseLog
 * @author: hebing
 * @create: 2023-08-21 15:08
 **/
@Builder
@Data
@AllArgsConstructor
public class HbaseLog implements Serializable {
    //日志批次号
    private String logBatchNo;
    //日志操作用户Id
    private String userId ;
    //日志对应表名
    private String logTableName;
    //当前时间戳
    private String logTime;

    //设置其他字段默认值
    public HbaseLog(String logBatchNo) {
        this.logBatchNo = logBatchNo;
        this.userId = "";
        this.logTableName = "hbase_log";
        this.logTime = String.valueOf(System.currentTimeMillis());
    }
}

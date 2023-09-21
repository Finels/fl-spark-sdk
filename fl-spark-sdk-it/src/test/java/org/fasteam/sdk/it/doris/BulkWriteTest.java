package org.fasteam.sdk.it.doris;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fasteam.doris.entry.Doris;
import org.fasteam.sdk.core.RuntimeContext;
import org.fasteam.sdk.core.SparkProcessor;
import org.fasteam.sdk.core.SparkUserDefineApplication;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description:  org.fasteam.sdk.it.doris
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/15
 */
public class BulkWriteTest extends SparkUserDefineApplication {
    @Override
    public void process(RuntimeContext argsContext) throws Exception {
        Calendar ca = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(int s=0;s<1024;s++) {
            List<DataBean> lst = new ArrayList<>();
            for (int i = 0; i < 500 * 1024; i++) {
                DataBean put = new DataBean();
                //一条数据10个字段
                put.setFactory_id("10100"+new Random().nextInt(10));
                put.setLevel(new Random().nextInt(5)+1+"");
                put.setShift(new Random().nextInt(2)+1+"");
                put.setProduct("P"+new Random().nextInt(8)+1);
                put.setSerial_num(UUID.randomUUID().toString());
                put.setPass_time(format.format(ca.getTime()));
                put.setPass(new Integer(new Random().nextInt(2)).byteValue());
                put.setCheck(new Integer(new Random().nextInt(2)).byteValue());
                put.setCreate_time(format.format(ca.getTime()));
                put.setCreate_user(UUID.randomUUID().toString());
                lst.add(put);
            }
            Dataset<Row> ds = SparkProcessor.getSession().createDataFrame(lst,DataBean.class);
            ds = ds.selectExpr("factory_id","level","shift","product","serial_num","pass_time","pass","check","create_time","create_user");
            ds.printSchema();
            Doris.write(ds,"scs.bigdata_partition_test");
            System.out.println("完成"+(s+1)+"次插入");
            ca.add(Calendar.DAY_OF_MONTH,new Random().nextInt(1));
        }
    }
}

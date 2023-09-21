package org.fasteam.sdk.it.doris;

import lombok.Data;

import java.util.Date;

/**
 * Description:  org.fasteam.sdk.it.doris
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/15
 */
@Data
public class DataBean {
    private String factory_id;
    private String level;
    private String shift;
    private String product;
    private String serial_num;
    private String pass_time;
    private byte pass;
    private byte check;
    private String create_time;
    private String create_user;
}

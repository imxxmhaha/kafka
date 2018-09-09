package cn.xxm.kafka.config;

import lombok.Data;

import java.util.Date;

/**
 * @author xxm
 * @create 2018-09-09 21:26
 */
@Data
public class Message {
    private String id;
    private String msg;
    private Date sendTime;
}

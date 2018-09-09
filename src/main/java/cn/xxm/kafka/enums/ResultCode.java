package cn.xxm.kafka.enums;

import lombok.Data;
import sun.applet.Main;

/**
 * @author xxm
 * @create 2018-09-09 16:57
 */
public enum ResultCode {
    SUCCESS(0,"发送kafka成功"),
    EXCEPTION(-1,"发送kafka成功");


    private Integer status;
    private String msg;

    ResultCode(Integer status, String msg) {
        this.status = status;
        this.msg = msg;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}

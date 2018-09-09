package cn.xxm.kafka.enums;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * @author xxm
 * @create 2018-09-09 16:56
 */
@Data
@AllArgsConstructor
public class Response {
    private Integer status;
    private String msg;
    private Map<String,Object> map;


}

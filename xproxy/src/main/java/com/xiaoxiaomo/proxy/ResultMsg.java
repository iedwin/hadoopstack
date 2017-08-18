package com.xiaoxiaomo.proxy;

import java.util.HashMap;
import java.util.Map;

/**
 * 状态码
 * 2xx：请求成功，并返回信息
 * 3xx：完成此请求必须进一步处理
 * 4xx：请求包含一个错误语法或不能完成
 * 5xx：服务器执行一个完全有效请求失败
 * Created by xiaoxiaomo on 2017/2/22.
 */
public class ResultMsg {

    public static final String MESSAGE_KEY = "msg";
    public static final String CODE_KEY = "status";
    public static final String DATA_KEY = "data";

    public enum StatusCode {

        SUCCESS(1, "成功"),
        FAIL(2, "失败");

        private int status;
        private String message;

        StatusCode(final int status, final String message) {
            this.status = status;
            this.message = message;
        }

        public static String getMessage(final int status) {
            for (StatusCode c : StatusCode.values()) {
                if (c.getStatus() == status) {
                    return c.message;
                }
            }
            return null;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }


    /**
     * 返回成功信息和数据
     */
    public static Map<String, Object> info(final Object status, final String message, final Object data) {
        Map<String, Object> map = new HashMap<>();
        map.put(CODE_KEY, status);
        map.put(MESSAGE_KEY, message);
        map.put(DATA_KEY, data);
        return map;
    }

    /**
     * 返回成功信息和数据
     */
    public static Map<String, Object> success(final Object data) {
        Map<String, Object> map = new HashMap<>();
        map.put(CODE_KEY, StatusCode.SUCCESS.getStatus());
        map.put(MESSAGE_KEY, StatusCode.getMessage(StatusCode.SUCCESS.getStatus()));
        map.put(DATA_KEY, data);
        return map;
    }


    /**
     * 返回错误信息
     */
    public static Map<String, Object> fail(final String msg) {
        Map<String, Object> map = new HashMap<>();
        map.put(CODE_KEY, StatusCode.FAIL.getStatus());
        map.put(MESSAGE_KEY, msg);
        return map;
    }


}

package com.xiaoxiaomo.mr.constants;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * 定义具体表信息
 * Created by xiaoxiaomo on 2017/6/1.
 */
public interface ConstantsTableInfo {
    String LINE_CHAR = String.valueOf((char) 0x09);
    String TABLE_NAME = "testTable"; //表名
    String COLUMN_FAMILY = "d";

    //淘宝用户信息
    List<String> TABLE_COLUMN = ImmutableList
            .<String>builder()
            .add("uid")
            .add("loginName")
            .add("nickName")
            .add("sex")
            .add("createTime")
            .add("mobile")
            .add("email")
            .add("updateTime")
            .build();

}

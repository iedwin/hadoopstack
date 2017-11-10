package com.xiaoxiaomo.spark.streaming.option;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 * Created by xiaoxiaomo on 2017/11/3.
 */
public interface DictOfTable {

    List<String> TEST1_COLUMN = ImmutableList
            .<String>builder()
            .add("id")
            .add("age")
            .add("name")
            .build();

    List<String> TEST2_COLUMN = ImmutableList
            .<String>builder()
            .add("id")
            .add("age")
            .add("name")
            .build();

    List<String> TEST3_COLUMN = ImmutableList
            .<String>builder()
            .add("id")
            .add("age")
            .add("name")
            .add("createTime")
            .add("updateTime")
            .build();

}

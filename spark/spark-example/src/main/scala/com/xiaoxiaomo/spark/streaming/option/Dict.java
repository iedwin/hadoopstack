package com.xiaoxiaomo.spark.streaming.option;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 *
 * Created by xiaoxiaomo on 2016/5/24.
 */
public interface Dict {


    Map<String, List<String>> STREAMING_TABLE = ImmutableMap
            .<String, List<String>>builder()
            .put("test1", DictOfTable.TEST1_COLUMN)
            .put("test2", DictOfTable.TEST2_COLUMN)
            .put("test3", DictOfTable.TEST3_COLUMN)
            .build();

}

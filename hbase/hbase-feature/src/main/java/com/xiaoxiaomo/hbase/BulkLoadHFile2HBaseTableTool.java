package com.xiaoxiaomo.hbase;

import com.xiaoxiaomo.hbase.utils.HFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 加载HFile文件到HBaseTable
 * Created by xiaoxiaomo on 2016/9/29.
 */
public class BulkLoadHFile2HBaseTableTool {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadHFile2HBaseTableTool.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            LOG.error("ERROR: Wrong number of parameters: " + args.length );
            LOG.error("Usage: VideoIndexer <hFilePath> <tableName> <family>");
            System.exit(-1);
        }

        String hFilePath = args[0];
        String tableName = args[1];
        String family = args[2];


        try {
            HFileLoader.doBulkLoad(hFilePath, tableName , family);//导入数据
            System.exit(0);
        } catch ( Exception e){
            System.exit(-1);
        }

    }
}

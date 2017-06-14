package com.xiaoxiaomo.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 通过BulkLoad 加载HFile文件到HBase
 * Created by TangXD on 2016/9/29.
 */
public class HFileLoader {
    private static final Logger LOG = LoggerFactory.getLogger(HFileLoader.class);

    /**
     * BulkLoad加载HFile文件
     * @param path 文件路径
     * @param tableName 加载到目标表
     */
    public static void doBulkLoad(String path, String tableName , String family){
        try {
            //加载配置
            Configuration conf = new Configuration();

            //设置MAX_FILES_PER_REGION_PER_FAMILY 防止异常：
            //Trying to load more than 32 hfiles to one family of one region
            conf.set(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY , "1024");
            HBaseConfiguration.addHbaseResources(conf);

            //导入数据
            LoadIncrementalHFiles loadHFiles = new LoadIncrementalHFiles(conf);

            //设置权限
            setPerm(new Path(path + "/" + family ) , conf);

            loadHFiles.doBulkLoad(new Path(path), IndexUtils.getHTable( tableName , false ) );
            LOG.info("Bulk Load Completed...");
        } catch(Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage() , e );
        }
    }


    /**
     * 授权目录及目录下文件
     * @param path 目录/d
     * @param conf 配置
     * @throws IOException
     */
    private static void setPerm(Path path  , Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        //设置权限
        if( fs.exists(path) ){
            fs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));//d
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path,true);
            while (listFiles.hasNext()) {
                LocatedFileStatus locatedFileStatus = listFiles.next();
                Path path1 = locatedFileStatus.getPath();
                fs.setPermission(path1, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
            }
        }
    }

}

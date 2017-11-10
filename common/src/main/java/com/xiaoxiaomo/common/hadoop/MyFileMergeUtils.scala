package com.xiaoxiaomo.common.hadoop

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

/**
  *
  * 文件操作
  *
  * Created by xiaoxiaomo on 2017/10/17.
  */
object MyFileMergeUtils {

    def mergeDirFiles(srcFS: FileSystem, srcDir: String): Unit ={

        val HDFSConf = new Configuration()
        val src = new Path(srcDir)
        val tmpFile = new Path(srcDir.substring(0,srcDir.lastIndexOf("/"))+"/tmp/d")
        val tmpPath = new Path(srcDir.substring(0,srcDir.lastIndexOf("/"))+"/tmp")
        val dst = new Path(srcDir+"/d")
        try{
            FileUtil.copyMerge(srcFS, src, srcFS, tmpFile, true, HDFSConf, null)
            FileUtil.copyMerge(srcFS, tmpPath, srcFS, dst, true, HDFSConf, null)
        }catch {
            case e: IOException =>
                e.printStackTrace()
        }
    }


    /**
      * 合并文件，支持指定大小
      * @param srcFS
      * @param srcDir
      * @param size
      */
    def mergeDirFiles(srcFS: FileSystem, srcDir: String, size:Long): Unit ={

        val HDFSConf = new Configuration()
        val src = new Path(srcDir)
        val dst = new Path(srcDir+"/data")
        try
            com.xiaoxiaomo.common.fs.FileUtilsExt.copyMerge(srcFS, src, srcFS, dst, true, HDFSConf, null,size)
        catch {
            case e: IOException =>
                e.printStackTrace()
        }
    }


    /**
      * eg:
      *  /opt/cloudera/parcels/CDH/bin/spark-submit --com.xiaoxiaomo.common.hadoop.FileUtils  hspark-1.0.jar path
      * @param args
      */
    def main(args: Array[String]): Unit = {

        if( args.length == 1 ){
            val HDFSConf = new Configuration()
            val fileSystem = FileSystem.get(HDFSConf)
            mergeDirFiles(fileSystem,args(0))
        }

        if( args.length == 2 ){
            val HDFSConf = new Configuration()
            val fileSystem = FileSystem.get(HDFSConf)
            mergeDirFiles(fileSystem,args(0),args(1).toLong)
        }
    }

}

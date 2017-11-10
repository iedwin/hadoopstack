package com.xiaoxiaomo.common.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 *
 * 对 org.apache.hadoop.fs.FileUtil 的copyMerge方法 进行改进
 *
 * Created by TangXD on 2017/11/6.
 */
public class FileUtilsExt {

    /** Copy all files in a directory to one output file (merge). */
    public static boolean copyMerge(FileSystem srcFS, Path srcDir,
                                    FileSystem dstFS, Path dstFile,
                                    boolean deleteSource,
                                    Configuration conf, String addString , long size) throws IOException {

        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return false;

        LinkedHashMap<Integer, ArrayList<Path>> pathLists = getDirMergeState(srcFS,srcDir,size);
        for (Integer integer : pathLists.keySet()) {
            ArrayList<Path> paths = pathLists.get(integer);

            OutputStream out = dstFS.create(
                    checkDest(
                            srcDir.getName()+integer,
                            dstFS,
                            new Path(dstFile.toUri().getPath()+integer.toString()),
                            false));

            try {
                for (Path path : paths) {
                    InputStream in = srcFS.open(path);
                    try {
                        IOUtils.copyBytes(in, out, conf, false);

                        if (addString != null)
                            out.write(addString.getBytes("UTF-8"));

                    } finally {
                        in.close();
                        if (deleteSource) {
                            srcFS.delete(path, true);
                        }
                    }
                }
            } finally {
                out.close();
            }
        }
        return true;
    }


    /**
     * 获取一个合并的数组
     * @param srcFS
     * @param srcDir
     * @param size
     * @return
     * @throws IOException
     */
    private static LinkedHashMap<Integer, ArrayList<Path>> getDirMergeState(FileSystem srcFS,Path srcDir,long size) throws IOException {
        LinkedHashMap<Integer, ArrayList<Path>> linkedHashMap = new LinkedHashMap<>();
        FileStatus[] contents = srcFS.listStatus(srcDir);
        Arrays.sort(contents);
        int key = 0 ;
        long currSize = 0;
        ArrayList<Path> paths = new ArrayList<>();
        for (int i = 0; i < contents.length; i++) {
            if (contents[i].isFile()) {
                long len = contents[i].getLen();
                if (len >= size) {
                    ArrayList<Path> path = new ArrayList<>();
                    path.add(contents[i].getPath());
                    linkedHashMap.put(key++, path);
                    continue;
                }

                if (currSize + len >= size) {
                    currSize = len;
                    linkedHashMap.put(key++, paths);
                    paths = new ArrayList<>();
                } else {
                    currSize += len;
                }
                paths.add(contents[i].getPath());
            }
        }
        if(0 < paths.size()){
            linkedHashMap.put(key++ , paths) ;
        }
        return linkedHashMap ;
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
                                  boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDirectory()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }
                return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
            } else if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }

}

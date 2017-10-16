package com.xiaoxiaomo.hadoop.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 *
 * 获取HA的地址
 * 1. 通过zk path获取,然后解析出hostname【如下】
 * 2. 连接捕获异常【略】
 * Created by TangXD on 2017/10/16.
 */
public class HDFSHAService {


    private static Logger log = Logger.getLogger(HDFSHAService.class.getName());

    private static final String ZOOKEEPER_IP = "10.141.5.20,10.141.5.25,10.141.5.26";
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int ZOOKEEPER_TIMEOUT = 30000;
    private static final String DATA_DIR = "/hadoop-ha/nameservice1/ActiveStandbyElectorLock"; //ZK HA PATH

    public static void main(String[] args) {
        String hostname = getHostname(ZOOKEEPER_IP, ZOOKEEPER_PORT, ZOOKEEPER_TIMEOUT, DATA_DIR);
        System.out.println(hostname);
    }

    /**
     * ͨ通过zk的数据解析出 hdfs 地址
     * @return
     */
    public static String getHostname(String ZOOKEEPER_IP, int ZOOKEEPER_PORT,
                                     int ZOOKEEPER_TIMEOUT,String DATA_DIR) {
        String hostname = null;
        Watcher watcher = new Watcher() {
            public void process(org.apache.zookeeper.WatchedEvent event) {
                log.info("event:"+event.toString());
            }
        };
        ZooKeeper zk ;
        byte[] data1 ;
        String[] data = ZOOKEEPER_IP.split(";");
        for (String ip : data) {
            try {
                zk = new ZooKeeper(ip + ":" + ZOOKEEPER_PORT,
                        ZOOKEEPER_TIMEOUT, watcher);
                data1 = zk.getData(DATA_DIR,true, new Stat());
            } catch (Exception e) {
                log.info("This ip is not active..."+ip);
                continue;
            }
            if (data1 != null) {
                log.info("This ip is normal..."+ip);
                ActiveNodeInfo activeNodeInfo=null;
                try {
                    activeNodeInfo = HAZKInfoProtos.ActiveNodeInfo.parseFrom(data1);
                } catch (InvalidProtocolBufferException e) {
                    log.error(e);
                }
                hostname = activeNodeInfo.getHostname();
                return hostname;
            }
        }
        return hostname;
    }
}

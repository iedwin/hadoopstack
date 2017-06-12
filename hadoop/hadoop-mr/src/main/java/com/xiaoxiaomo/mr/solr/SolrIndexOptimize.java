package com.xiaoxiaomo.mr.solr;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * 优化索引
 *
 * Created by xiaoxiaomo on 2016/9/8.
 */
public class SolrIndexOptimize {

    private static final Logger logger = LoggerFactory.getLogger(SolrIndexOptimize.class);
    public static void main(String[] args) {
        //优化索引
        CloudSolrServer cloudSolrServer =
                HBaseRowKeyToSolrJob.cloudSolrServer;
        try {
            cloudSolrServer.optimize();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("索引优化 error ： " , e.getMessage());
        }
    }
}

package com.xiaoxiaomo.mr.solr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * 从HBase 导入数据到 Solr
 *
 * Created by xiaoxiaomo on 2016/9/1.
 */
public class HBaseRowKeyToSolrJob extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(HBaseRowKeyToSolrJob.class);
    private static final Properties prop ;
    public static CloudSolrServer cloudSolrServer;
    static  {
        prop = new Properties();
        InputStream in = HBaseRowKeyToSolrJob.class.getResourceAsStream( "/solr.properties" );
        try  {
            prop.load(in);
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, prop.getProperty("solr_max_total_connections"));//1000
            params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, prop.getProperty("solr_max_connections_per_host"));//1000
            params.set(HttpClientUtil.PROP_SO_TIMEOUT,prop.getProperty("solr_so_timeout"));//1分钟
            params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT,prop.getProperty("solr_connection_timeout"));//1分钟
            params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS,prop.getProperty("solr_follow_redirects"));//是否允许从定向，false
            params.set(HttpClientUtil.PROP_ALLOW_COMPRESSION,prop.getProperty("solr_allow_compression"));//是否允许压缩，true
            HttpClient client = HttpClientUtil.createClient(params);
            LBHttpSolrServer lbServer = new LBHttpSolrServer(client);

            cloudSolrServer = new CloudSolrServer( prop.getProperty("solr_zk_host") , lbServer);
            cloudSolrServer.setDefaultCollection( prop.getProperty("solr_scan_collection"));
            cloudSolrServer.setZkClientTimeout(Integer.valueOf( prop.getProperty("solr_zk_client_timeout") ));
            cloudSolrServer.setZkConnectTimeout(Integer.valueOf( prop.getProperty("solr_zk_connect_timeout") ));
            cloudSolrServer.setParser( new XMLResponseParser() );

        } catch (Exception e) {
            throw new RuntimeException("加载配置文件出错");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);//推测执行
        conf.setInt("hbase.client.scanner.caching", 1000);//一次next()从服务器获取数据条数

        //参数，需要传入表名
        if (args.length < 2) {
            System.err.println("ERROR: Wrong number of parameters: " + args.length );
            System.err.println("Usage: VideoIndexer <tableName> <family>");
            System.exit(-1);
        }

        String tableName = args[0];
        String family = args[1];
        conf.set("tb_family", tableName + "_" +  family);

        Job job = new Job(conf, "SolrIndex_" + tableName  +"_"+family + " Job @ " );
        job.setJarByClass(HBaseRowKeyToSolrJob.class);

        Scan scan = new Scan();
        scan.setCacheBlocks(false); // scan的数据不放在缓存中，一次性的
        scan.addFamily(family.getBytes());

        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(
                tableName, scan, SolrIndexerMapper.class, null, null, job); //不需要输出
        job.setNumReduceTasks(0); // 无reduce任务

        if ( job == null ) {
            System.exit(-1);
        }

        //提交
        return  job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper实现
     */
    public static class SolrIndexerMapper extends TableMapper<Text, Text> {
        private final List<SolrInputDocument> inputDocs = new ArrayList<>();//数据
        private String tb_family;
        private int commitSize;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            tb_family =  conf.get("tb_family");
            commitSize = conf.getInt("solr.commit.size", 50000); // 一次性添加的文档数
        }

        @Override
        public void map(ImmutableBytesWritable row, Result values, Mapper.Context context)
                throws IOException {
            SolrInputDocument solrDoc = new SolrInputDocument() ;
            solrDoc.addField("tb_family", tb_family );
            solrDoc.addField("rowkey", new String(values.getRow()));
            inputDocs.add(solrDoc);

            if (inputDocs.size() >= commitSize) {
                try {
                    logger.info("添加索引： " + Integer.toString(inputDocs.size()) + " documents");
                    cloudSolrServer.add(inputDocs,300000); // 索引文档,5五分钟提交
                    cloudSolrServer.commit();
                } catch (SolrServerException e) {
                    e.printStackTrace();
                    logger.error("cleanup error ： " , e);
                }
                inputDocs.clear();
            }
        }


        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            if (!inputDocs.isEmpty()) {
                logger.info("清空队列： " + Integer.toString(inputDocs.size()) + " documents");
                try {
                    cloudSolrServer.add(inputDocs);
                    cloudSolrServer.commit();
                    inputDocs.clear();
                } catch (SolrServerException e) {
                    e.printStackTrace();
                    logger.error("cleanup error ： " , e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseRowKeyToSolrJob(), args);
        System.exit(exitCode);
    }
}

package com.xiaoxiaomo.spark.streaming.kafka

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.xiaoxiaomo.common.hadoop.MyFileMergeUtils
import com.xiaoxiaomo.spark.streaming.option.Dict
import com.xiaoxiaomo.spark.streaming.util.StreamingUtils
import com.xiaoxiaomo.utils.DateUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}

/**
  * spark consumer 消费kafka数据 然后计算
  *
  * 运行示例，具体见配置文件：
  * /opt/cloudera/parcels/CDH/bin/spark-submit --master yarn-client --class com.xiaoxiaomo.spark.streaming.kafka.Kafka2HiveStreaming.jar
  *
  */
object Kafka2HiveStreaming {

    private val LOG: Logger = LoggerFactory.getLogger(Kafka2HiveStreaming.getClass)

    def main(args: Array[String]) {

        val Array(group , topics, timeWindow, maxRatePerPartition) = args
        val sc = StreamingUtils.getSparkContext(this.getClass.getSimpleName, maxRatePerPartition)
        val ssc = StreamingUtils.getStreamingContext( sc, timeWindow.toInt)
        val hiveContext = StreamingUtils.getHiveContext( sc )
        val HDFSConf = new Configuration()
        val messages = StreamingUtils.createDirectStream(ssc,group,topics)

        //数据操作，解析
        //rdd就是所有的数据
        messages.foreachRDD( rdd => {
            val data = rdd.map( x=> {
                var json = new JSONObject()
                var topic : String = null
                try {
                    val jsonObj = JSON.parseObject(x._2)
                    if ( !jsonObj.isEmpty && jsonObj.size() > 0  && jsonObj.containsKey("q") && jsonObj.containsKey("v")){
                        val q = jsonObj.getJSONArray("q")
                        val v = jsonObj.getJSONArray("v")
                        for ( i <- 0 until q.size() )   if (q.get(i).toString.equals("event")) topic = v.get(i).toString()
                        json = analysisJson(topic,q,v)
                    }
                } catch{
                    case e : Exception => {
                        LOG.warn("exception：",x._2)
                        LOG.warn(e.getMessage,e)
                    }
                }

                (topic,json)

            }).groupByKey().collect()


            // foreach 上面的key group
            data.foreach( value =>  {
                if(  Dict.STREAMING_TABLE.containsKey(value._1) ){ // 去掉异常key (null&&other event)
                    val array = new JSONArray()
                    val iterator = value._2.iterator
                    while ( iterator.hasNext )  array.add(iterator.next())

                    if ( !array.isEmpty && array.size() > 0){
                        val dataFrame = hiveContext.read.json(sc.parallelize(List(array.toString)))
                        dataFrame.registerTempTable( value._1+"_tmp")
                        // select.printSchema() //表结构
                        // select.show()

                        // 创建分区
                        val fileSystem = FileSystem.get(HDFSConf)
                        val dateNowStr = DateUtil.getNowDate
                        if (!fileSystem.exists(new Path("/user/hive/warehouse/xxx.db/table/dt="+dateNowStr))){
                            hiveContext.sql("ALTER TABLE xxx.table ADD PARTITION(dt='"+dateNowStr+"')")
                        }
                        // 合并文件
                        if ( System.currentTimeMillis() - DateUtil.getTimestamp(dateNowStr) < 500000 ) MyFileMergeUtils.mergeDirFiles(fileSystem,"/user/hive/warehouse/xxx.db/table/dt="+DateUtil.getDaysBefore(dateNowStr, "yyyy-MM-dd"))

                        // 加载数据
                        // 生产的文件数量为Reduce的个数*分区的个数
                        hiveContext.sql("insert into xxx.table partition(dt='"+dateNowStr+"') select * from " + value._1+"_tmp")
                    }
                }
            })

            val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd.foreachPartition(_ => {
                val offsetRange: OffsetRange = offsetsList(TaskContext.get.partitionId)
                // updateZkOffset
                StreamingUtils.updateZkOffset(group,offsetRange)

            })

        })

        ssc.start()
        ssc.awaitTermination()
    }



    def analysisJson( topic: String , keys: JSONArray , values: JSONArray ) : JSONObject = {
        val json = new JSONObject()
        var treeMap = new scala.collection.immutable.TreeMap[Int,Int]()
        val column = Dict.STREAMING_TABLE.get(topic)

        if( topic != null && column != null && keys != null ){

            for (i <- 0 until keys.size() ) {
                val indexOf = column.indexOf(keys.get(i))
                if( indexOf != -1 ) treeMap += ( indexOf -> i )
            }

            // streaming to json
            var value : String = null
            for (i <- 0 until column.size()) {
                if (treeMap.contains(i) && values.get(treeMap(i)) != null ) {
                    value = values.getString(treeMap(i))
                } else {
                    value = "-"
                }
                json.put(column.get(i),value)
            }
        }
        return json
    }
}

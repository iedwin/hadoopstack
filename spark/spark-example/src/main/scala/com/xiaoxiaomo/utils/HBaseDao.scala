package com.xiaoxiaomo.utils

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.LogManager

import scala.util.Try

/**
  * HBase 数据库操作
  *
  * Created by TangXD on 2017/4/20.
  */
object HBaseDao extends Serializable {

    @transient lazy val logger = LogManager.getLogger(HBaseDao.getClass)

    private val IDX_TB_SUFFIX: String = "Idx"
    private val HBASE_IDX_TABLE: Set[String] = hBaseIdx
    private val connection: Connection = createHBaseConn

    def createHBaseConn: Connection = {
        val conf: Configuration = HBaseConfiguration.create()
        conf.addResource(this.getClass().getResource("/develop/hbase-site.xml"))
        ConnectionFactory.createConnection(conf)
    }

    def hBaseIdx: Set[String] = {
        val props: Properties = new Properties()
        props.load(this.getClass().getResourceAsStream("/develop/server.properties"))
        props.getProperty("hbase.index").split(",").toSet
    }


    /**
      * 获取表
      *
      * @param tableName
      * @return
      */
    def getTable(tableName: String): HTable = {
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        table.asInstanceOf[HTable]
    }

    /**
      *
      * 插入数据到 HBase
      *
      * 参数( tableName , [( tableName , json )] )：
      * Json格式：
      * {
      * "r": "00000-0",
      * "f": "d",
      * "q": [
      * "customerId"
      * ],
      * "v": [
      * "0"
      * ],
      * "t": "1494558616338"
      * }
      *
      * @return
      */
    def insert(tableName: String, array: Iterator[(String, String)]): Boolean = {

        try {
            /** 操作数据表 && 操作索引表 */
            val t: HTable = getTable(tableName) //HTable

            var tIdx: HTable = null
            var hasIdxTable = false
            if (HBASE_IDX_TABLE.contains(tableName + IDX_TB_SUFFIX)) {
                tIdx = getTable(tableName + IDX_TB_SUFFIX) //HTable
                hasIdxTable = true
            }

            val puts: util.ArrayList[Put] = new util.ArrayList[Put]()
            val putsIdx: util.ArrayList[Put] = new util.ArrayList[Put]()

            /** 遍历Json数组 */
            array.foreach(json => {

                val jsonObj: JSONObject = JSON.parseObject(json._2)

                val rowKey: Array[Byte] = jsonObj.getString("r").getBytes
                val family: Array[Byte] = jsonObj.getString("f").getBytes
                val qualifiers: JSONArray = jsonObj.getJSONArray("q")
                val values: JSONArray = jsonObj.getJSONArray("v")

                val put = new Put(rowKey)
                for (i <- 0 until qualifiers.size()) {
                    put.addColumn(family, qualifiers.getString(i).getBytes, values.getString(i).getBytes)
                }

                puts.add(put)

                if (hasIdxTable) {
                    val putIdx = new Put(rowKey)
                    putIdx.addColumn(family, "".getBytes, "".getBytes)
                    putsIdx.add(putIdx)
                }
            })

            Try(t.put(puts)).getOrElse(t.close())
            if (hasIdxTable) {
                Try(tIdx.put(putsIdx)).getOrElse(tIdx.close())
            }
            true
        } catch {
            case e: Exception =>
                e.printStackTrace()
                logger.error(s"insert ${tableName} error ", e)
                false
        }
    }


}

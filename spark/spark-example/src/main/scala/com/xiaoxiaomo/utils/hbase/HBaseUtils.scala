package com.xiaoxiaomo.utils.hbase

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.LogManager

import scala.util.Try

/**
  * HBase 数据库操作
  *
  * Created by xiaoxiaomo on 2016/4/20.
  */
object HBaseUtils extends Serializable {

    @transient lazy val LOG = LogManager.getLogger(HBaseUtils.getClass)

    private val connection: Connection = createHBaseConnection

    def createHBaseConnection: Connection = {
        val conf: Configuration = HBaseConfiguration.create()
        conf.addResource(this.getClass().getResource("/hbase-site.xml"))
        ConnectionFactory.createConnection(conf)
    }

    /**
      * @param tableName
      * @return
      */
    def getTable(tableName: String): HTable = {
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        table.asInstanceOf[HTable]
    }

    def insert(tableName: String, array: Iterator[(String, String)]): Boolean = {

        try {
            val t: HTable = getTable(tableName)
            val puts: util.ArrayList[Put] = new util.ArrayList[Put]()

            // 遍历Json数组
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
            })

            Try(t.put(puts)).getOrElse(t.close())
            true
        } catch {
            case e: Exception =>
                e.printStackTrace()
                LOG.error(s"insert ${tableName} error ", e)
                false
        }
    }

}

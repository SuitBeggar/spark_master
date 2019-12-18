package sparksql

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * @description:
 * @Author:suitbeggar
 * @Date:2019/12/17 23:33
 * @Version:
 **/
object SparkstreamToSqlToHbase {
  def main(args: Array[String]): Unit = {

    val conf = new  SparkConf().setAppName("SparkstreamToSqlToHbase").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val streaming = new StreamingContext(sc,Seconds(10))

    val lines : ReceiverInputDStream[String] = streaming.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

    lines.foreachRDD((rdd:RDD[String],time:Time) =>{
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val wordsDataFrame = rdd.map{w =>
        (w.split(" ")(0),w.split(" ")(1),w.split(" ")(2))
      }.map(x => (x._1,x._2,x._3)).toDF("key","col","val")

      wordsDataFrame.registerTempTable("words")

      val wordcountDataFrame = wordsDataFrame.sqlContext.sql("select key,col,val from words")

      wordcountDataFrame.show();

      wordcountDataFrame.foreach(x => HbaseHandler.insert(
        x.getAs[String]("key"),
        x.getAs[String]("col"),
        x.getAs[String]("val")))
    })


    streaming.start()
    streaming.awaitTermination()
  }
}


object HbaseHandler {
  def insert(row: String, column: String, value: String) {
    // Hbase配置
    val tableName = "sparkstream_sql_hbase_table" // 定义表名
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val hTable = new HTable(hbaseConf, tableName)
    val thePut = new Put(row.getBytes)
    thePut.add("info".getBytes,column.getBytes,value.getBytes)
    hTable.setAutoFlush(false, false)
    // 写入数据缓存
    hTable.setWriteBufferSize(3*1024*1024)
    hTable.put(thePut)
    // 提交
    hTable.flushCommits()
  }
}
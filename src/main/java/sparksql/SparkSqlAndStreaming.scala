package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fangyitao on 2019/12/16.
  */
object SparkSqlAndStreaming {

  def main(args: Array[String]): Unit = {

      val conf = new  SparkConf().setAppName("SparkSqlAndStreaming")

      val sc = new SparkContext(sc)

      val streaming = new StreamingContext(sc,Seconds(10))

      val lines : ReceiverInputDStream[String] = streaming.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

      val words : DStream[String] = lines.flatMap(_.split(" "))

      words.foreachRDD((rdd:RDD[String],time:Time) =>{
          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          //val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      })


  }

}

object SQLContextSingleton{
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

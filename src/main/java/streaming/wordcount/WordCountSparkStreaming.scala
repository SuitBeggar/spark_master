package streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming的wordcount实现(无状态，也就是不记录每次的结果)
  * Created by fangyitao on 2019/11/19.
  */
object WordCountSparkStreaming {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("WordCountSparkStreaming")

      val scc = new StreamingContext(conf,Seconds(15))

      val host = "master";

      val port = 9999
      //通过socet 接收数据来自 host port端口的数据
      val lines =  scc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)

      val words = lines.flatMap(_.split(" "))

      val wordcount = words.map(x => (x,1)).reduceByKey(_ + _)

      wordcount.print()

      wordcount.saveAsTextFiles("hdfs//master:9000/output","doc")

      scc.start()

      scc.awaitTermination()
  }
}

package streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming的wordcount实现(有状态，记录每次的结果)
  * Created by fangyitao on 2019/11/19.
  */
object WordCountSparkStreamingState {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("WordCountSparkStreamingState")

      val scc = new StreamingContext(conf,Seconds(15))

      //恢复上次计算
      scc.checkpoint("hdfs//master:9000/wordcount/checkpoint")

      //通过socet 接收数据来自 host port端口的数据
      val lines = scc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

      val  words = lines.flatMap(_.split(" "))

      val wordcounts = lines.map(x => (x,1)).reduceByKey(_ + _)

      wordcounts.print();

      wordcounts.saveAsTextFiles("hdfs//master:9000/output","doc")


      scc.start()

      scc.awaitTermination()
  }
}

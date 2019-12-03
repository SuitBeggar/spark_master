package streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming��wordcountʵ��(��״̬��Ҳ���ǲ���¼ÿ�εĽ��)
  * Created by fangyitao on 2019/11/19.
  */
object WordCountSparkStreaming {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("WordCountSparkStreaming")

      val scc = new StreamingContext(conf,Seconds(15))

      val host = "master";

      val port = 9999
      //ͨ��socet ������������ host port�˿ڵ�����
      val lines =  scc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)

      val words = lines.flatMap(_.split(" "))

      val wordcount = words.map(x => (x,1)).reduceByKey(_ + _)

      wordcount.print()

      wordcount.saveAsTextFiles("hdfs//master:9000/output","doc")

      scc.start()

      scc.awaitTermination()
  }
}

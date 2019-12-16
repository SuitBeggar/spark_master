package sparkstreaming.workcount
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming的wordcount实现
 * (Windows时间窗口模式统计，也就是统计一段时间内的结果)
 * @description:
 * @Author:bella
 * @Date:2019/12/1623:21
 * @Version:
 **/
object WordCountWindowsSparkStreaming {
  def main(args: Array[String]): Unit = {

    val  conf = new SparkConf().setAppName("WordCountWindowsSparkStreaming")

    val  scc = new StreamingContext(conf,Seconds(10))

    scc.checkpoint("hdfs//master:9000/wordcount/checkpoint")

    val lines = scc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))

    //每隔10秒,统计前40秒内每一个单词出现的次数
    val wordcounts = words.map(x => (x,1)).reduceByKeyAndWindow((v1 : Int,v2 : Int) => v1+v2,Seconds(40),Seconds(10))

    wordcounts.print()

    scc.start()

    scc.awaitTermination()

  }
}

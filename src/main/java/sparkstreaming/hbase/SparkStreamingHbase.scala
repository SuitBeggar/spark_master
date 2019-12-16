package sparkstreaming.hbase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming   =   >Hbase
 * @description:
 * @Author:bella
 * @Date:2019/12/1623:14
 * @Version:
 **/
object SparkStreamingHbase {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkStreamingHbase")

    val scc = new StreamingContext(conf,Seconds(10))

    scc.checkpoint("hdfs//master:9000/kafka/checkpoint")

    var zkQuorum = "master:9092,slave1:9092,slave2:9092"

    val group = "group_1"

    val numThreads = 1

    val topics = "topic_test"

    val topic_Map = topics.split(",").map((_,numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(scc,zkQuorum,group,topic_Map).map(_._2)


    //输入hbase的数据一行行的输入
    val word = lines.flatMap(_.split("\n"))

    //每一列使用|分割
    val words = word.map(_.split("\\|"))

    words.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionRdd =>{
        partitionRdd.foreach(pari =>{
          val key = pari(0)
          val col = pari(1)
          val value = pari(2)
          println(key + "_" + col + " : " + value)
        })
      })
    })

  }
}

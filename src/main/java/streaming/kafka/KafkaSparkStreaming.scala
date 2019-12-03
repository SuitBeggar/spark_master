package streaming.kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**SparkStreaming接收kafka数据（被动模式）Receiver
  * Created by fangyitao on 2019/11/19.
  */
object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaSparkStreaming")

    val scc = new StreamingContext(conf,Seconds(10))

    scc.checkpoint("hdfs//master:9000/kafka/checkpoint")

    var zkQuorum = "master:9092,slave1:9092,slave2:9092"

    val group = "group_1"

    val topic : String = "topic_test";

    //val lines = scc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

    //通过Receiver方式接收kafka的数据（被动接收）
    val topicAndLines:ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(scc,zkQuorum,group,Map(topic -> 1),StorageLevel.MEMORY_AND_DISK_SER)

    val lines : DStream[String] = topicAndLines.map(x => x._2)

    val arry = ArrayBuffer[String]()

    lines.foreachRDD(rdd => {
      val count = rdd.count().toInt

      rdd.take(count+1).take(count).foreach(x => {
        arry += x+ "-----read"
      })
      arry.clear()
    })



  }
}

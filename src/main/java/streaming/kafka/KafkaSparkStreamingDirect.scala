package streaming.kafka

import java.io.InputStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming����kafka���ݣ�����ģʽ��direct
  * Created by fangyitao on 2019/11/20.
  */
object KafkaSparkStreamingDirect {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaSparkStreamingDirect")

    val scc = new StreamingContext(conf,Seconds(10))

    scc.checkpoint("hdfs//master:9000/kafka/checkpoint")

    var zkQuorum = "master:9092,slave1:9092,slave2:9092"

    val group = "group_1"

    //�ɽ��ն��topic��ʹ�ö��ŷָ���
    val topic = "topic_test";

    val topicSet = topic.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum)

    val message : InputDStream[(String,String)]= KafkaUtils.createDirectStream(scc, kafkaParams,topicSet)

    val lines : DStream[String] = message.map(x => x._2)

    val words : DStream[String] = lines.flatMap(_.split(" "))

    val wordCounts : DStream[(String,Int)] = words.map(x =>(x,1)).reduceByKey(_ + _)

    wordCounts.print()

    scc.start()

    scc.awaitTermination()

  }
}

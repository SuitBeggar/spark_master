package streaming.kafka


import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/** spark作为生产者，为kafka提供数据
  * Created by fangyitao on 2019/11/19.
  */
object KafkaProducerSpark {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("KafkaProducerSpark")

      val scc = new SparkContext(conf)

      val  arry = ArrayBuffer("1111","2222","3333")
  }

  def ProducerSender(args:ArrayBuffer[String]) : Unit = {
      if(args != null){
        val brokers = "master:9092,slave1:9092,slave2:9092";

        val props = new util.HashMap[String,Object]()

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.erialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        val topic  = "topic_test";

        for(arg <- args){
          println("i have send message: " + arg)

          val message = new ProducerRecord[String, String](topic,null,arg)

          producer.send(message)

        }

        Thread.sleep(500)

        producer.close()

      }
  }
}

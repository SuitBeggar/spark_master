package join

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description:
  * @Author:bella
  * @Date:2019 /10/821:29
  * @Version:
  **/
object  JoinTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JoinTest")

    val sc = new SparkContext(conf)

    val input_1 = "aaa";

    val input_2 = "bbb";

    val rdd_1 = sc.textFile(input_1).map(line =>{
      val takens = line.split(" ")

      (takens(0),takens(1))
    }).cache()

    val rdd_2 = sc.textFile(input_2).map(line =>{
      var takens = line.split(" ")

      (takens(0),takens(1))
    })

    val rdd_3 = rdd_1.join(rdd_2)

    val rdd_4 = rdd_3.map(line =>{
      val name = line._1

      val address = line._2._1

      val phone = line._2._2

      (name,(address,phone))
    })

    rdd_4.saveAsTextFile("")
  }
}

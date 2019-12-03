package filter

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description:
  * @Author:bella
  * @Date:2019 /10/716:32
  * @Version:
  **/
object DataFilter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dataFilter")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("")
    //过滤长度小于0， 过滤不包含GET与POST的URL
    val rdd_1 = rdd.filter(_.length()>0).filter(line=>(line.indexOf("GET")>0||line.indexOf("POST")>0))

    rdd_1.saveAsTextFile("")
  }
}

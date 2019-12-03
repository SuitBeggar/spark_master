package cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @description:
  * @Author:bella
  * @Date:2019 /10/1621:29
  * @Version:
  **/
object CF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("recommendersystemCF")

    val sc = new SparkContext(conf)

    //val rdd = sc.textFile("hdfs://master:9000/cf/input/cf.data")
    val rdd = sc.textFile("hdfs://master:9000/recommendersystem/cf/input/cf_train.data")

    var step1 = rdd.map(x =>{
      val u_i_s = x.split(",")
      (u_i_s(0).toString,(u_i_s(1).split(":")(0).toString,u_i_s(1).split(":")(1).toString))
    }).groupByKey().flatMap(y =>{
        val userid =y._1
        val i_s_list = y._2
        var is_arr = i_s_list.toArray
        val is_len = is_arr.length
        var i_us_arr = new ArrayBuffer[(String, (String, Double))]

       for (i <- 0 until is_len){
            val itemid = is_arr(i)._1
            val score = is_arr(i)._2.toDouble
            i_us_arr +=  ((itemid,(userid,score)))
       }
      i_us_arr
    }).groupByKey().flatMap(x =>{
        val itemid = x._1
        val u_s = x._2
        var us_arr = u_s.toArray
        var sum = 0.0
        for(i<- 0 until us_arr.length){
          sum += pow(us_arr(i)._2.toDouble,2.0)
        }
        sum =sqrt(sum)

      var u_is_arr = new ArrayBuffer[(String, (String, Double))]

      for (i <- 0 until us_arr.length){
        val userid = us_arr(i)._1
        val score = us_arr(i)._2.asInstanceOf[Double]/sum.toDouble
        u_is_arr +=  ((userid,(itemid,score)))
      }
      u_is_arr
    }).groupByKey() //.saveAsTextFile("hdfs://master:9000/cf/output_spark")


   var step2 = step1.flatMap(x=>{
      val userid = x._1
      val i_s = x._2
      var is_arr = i_s.toArray

      var ii_s_arr = new ArrayBuffer[((String,String),Double)]

      for(i <- 0 until is_arr.length-1){
        for(j<- i+1 until is_arr.length){
          var itema = is_arr(i)._1
          var scorea = is_arr(i)._2

          var itemb = is_arr(j)._1
          var scoreb = is_arr(j)._2

          ii_s_arr += (((itema, itemb),scorea * scoreb))
          ii_s_arr += (((itemb, itema), scorea * scoreb))

        }
      }
      ii_s_arr
    }).groupByKey()

    val step3 = step2.map(x=>{
      val ii = x._1
      val s_list = x._2
      var score = 0.0

      val s_arr = s_list.toArray
      for(i<- 0 until s_arr.length){
            score += s_arr(i)
      }
      if(score.isNaN){
        score = 0.0
      }
      (ii._1,(ii._2,score.toString))
    }).saveAsTextFile("hdfs://master:9000/recommendersystem/cf/output_spark")

    //}).saveAsTextFile("hdfs://master:9000/cf/output_spark")

  }
}

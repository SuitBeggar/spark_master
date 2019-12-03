import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description:
  * @Author:bella
  * @Date:2019 /9/2522:30
  * @Version:
  **/
object userwatchlist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("userwatchlist");

    val sc = new SparkContext(conf);

    val rdd = sc.textFile("hdfs://master:9000/input/train_new.data");

    val rdd1 = rdd.filter{x =>
      val fileds = x.split("\t");
      fileds(2).toDouble>2
    }.map{x =>
      val fileds = x.split("\t");
      (fileds(0).toString,(fileds(1).toString,fileds(2).toString));
    }.groupByKey().map{ x =>
      val userid = x._1
      val list = x._2
      var temp = list.toArray.sortWith(_._2>_._2)
      var len = temp.length

      if(len>5){
        len = 5
      }

      val str = new StringBuffer()
      for(i <- 0 until(len)){
        str.append(temp(i)._1)
        str.append(" : ")
        str.append(temp(i)._2)
        str.append("  ")

      }
      (userid,str)
    }.sortBy(_._1)
    rdd1.saveAsTextFile("hdfs://master:9000/ouput/train_new.data")

  }
}

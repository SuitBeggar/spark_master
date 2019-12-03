import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description:
  * @Author:bella
  * @Date:2019 /9/23 20:55
  * @Version:
  **/
object wordcount {
  def main(args: Array[String]): Unit = {
    //.setMaster("local[2]")
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val sc = new SparkContext(conf)

    //var rdd = sc.textFile("/root/spark/exmple1/1.data")
    var rdd = sc.textFile("hdfs://master:9000/input/1.data")
    val data = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).map { x =>
      x._1 + "\t" + x._2
    }

    //data.saveAsTextFile("/root/spark/exmple1/wordcount_out")

    data.saveAsTextFile("hdfs://master:9000/output/wordcount_output")
  }
}

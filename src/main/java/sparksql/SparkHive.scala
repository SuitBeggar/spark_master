package sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fangyitao on 2019/12/16.
  */
object SparkHive {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("SparkHive")

      val sc = new SparkContext(conf)

      val hiveContext = new HiveContext(sc)

      hiveContext.table("movie_table").registerTempTable("movie_table")

      hiveContext.sql("select * from movie_table").show()

  }
}

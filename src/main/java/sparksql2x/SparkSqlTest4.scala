package sparksql2x

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @description:
 * @Author:suitbeggar
 * @Date:2019/12/19 0:42
 * @Version:
 **/
object SparkSqlTest4 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSqlTest4")
      .getOrCreate()

    import session.implicits._
    val lines: Dataset[String] = session.read.textFile("hdfs://master:9000/The_Man_of_Property.txt")

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //val count = words.groupBy($"value" as "word").count()

    val count = words.groupBy($"value" as "word").count().sort($"count" desc)

    count.show()

    session.stop()

  }
}

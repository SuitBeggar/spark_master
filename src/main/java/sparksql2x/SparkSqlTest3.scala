package sparksql2x

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @description:
 * @Author:suitbeggar
 * @Date:2019/12/19 0:24
 * @Version:
 **/
object SparkSqlTest3 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSqlTest3")
      .getOrCreate()

    val lines : Dataset[String] = session.read.textFile("hdfs://master:9000/The_Man_of_Property.txt")

    import session.implicits._
    val words : Dataset[String]= lines.flatMap(_.split(" "))

    words.createTempView("words")

    val result = session.sql("select value, count(*) as counts from wc_table group by value")

    result.show()

    session.stop()

  }
}

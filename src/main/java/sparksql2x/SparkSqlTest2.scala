package sparksql2x

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/**
 * @description:
 * @Author:suitbeggar
 * @Date:2019/12/19 0:16
 * @Version:
 **/
object SparkSqlTest2 {

    def main(args: Array[String]): Unit = {

      val session = SparkSession.builder()
        .master("local[*]")
        .appName("SparkSqlTest2")
        .getOrCreate()

      val lines = session.sparkContext.textFile("hdfs://master:9000/sql_stu.data")

      val stuRDD = lines.map(line =>{
        val fileds = line.split(',')

        val no = fileds(0).toLong
        val name = fileds(1).toString
        val sex = fileds(2).toInt
        val age = fileds(3).toInt
        val cla = fileds(4).toInt

        Row(no,name,sex,age,cla)
      })

      val StudentSchema: StructType = StructType(List(
        StructField("Sno", LongType, nullable = true),
        StructField("Sname", StringType, nullable = true),
        StructField("Ssex", IntegerType, nullable = true),
        StructField("Sbirthday", IntegerType, nullable = true),
        StructField("SClass", IntegerType, nullable = true)
      ))

      val df: DataFrame = session.createDataFrame(stuRDD,StudentSchema)

      import session.implicits._

      val df2 : Dataset[Row] = df.where($"Sno" > 102).orderBy($"SClass")

      df2.show()

      session.stop()

    }
}

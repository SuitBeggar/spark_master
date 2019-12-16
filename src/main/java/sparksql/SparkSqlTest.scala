package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by fangyitao on 2019/12/16.
  */
object SparkSqlTest {

  def main(args: Array[String]): Unit = {

    val conf  = new SparkConf().setAppName("SparkSqlTest")

    val sc = new SparkContext(conf);

    val sqlContext = new SQLContext(sc);

    val studentDatas : RDD[String] = sc.textFile("hdfs://master:9000/sql_stu.data")

    val studentData : RDD[Row] = studentDatas.map{
          lines =>
          val line = lines.split(",")
          Row(line(0),line(1),line(2),line(3),line(4))
    }

    val StudentSchema:StructType = StructType(mutable.ArraySeq(
        StructField("sno",StringType,nullable = false),
        StructField("sname",StringType,nullable = false),
        StructField("ssex",StringType,nullable = false),
        StructField("sbirthday",StringType,nullable = true),
        StructField("sclass",StringType,nullable = true)
    ))

    val StudentTable : DataFrame = sqlContext.createDataFrame(studentData,StudentSchema)

    StudentTable.registerTempTable("student")

    sqlContext.sql("select * from student").show()

  }

}

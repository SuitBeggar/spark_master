package sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fangyitao on 2019/12/16.
  */
object SparkSqlJson {

  def main(args: Array[String]): Unit = {

    val conf  = new SparkConf().setAppName("SparkSqlJson").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val personInfo : DataFrame = sqlContext.read.json("hdfs://master:9000/sparksql/person_info.json")

    personInfo.registerTempTable("personInfo")

    sqlContext.sql("select * from personInfo").show()

    print(personInfo.schema)
  }
}

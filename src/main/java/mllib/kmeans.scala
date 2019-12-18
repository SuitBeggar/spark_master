package mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @Author:suitbeggar
 * @Date:2019/12/18 23:32
 * @Version:
 **/
object kmeans {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setMaster("local[2]").setAppName("kmeans")

      val sc = new SparkContext(conf)

      val rawTrainingData = sc.textFile(args(0))

      val parsedTestData = rawTrainingData.filter(!isColumnNameLine(_))
        .map(line =>{
          Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
        })

      parsedTestData.collect().foreach(testDataLine => {
        val predictedClusterIndex:
          Int = clusters.predict(testDataLine)
        println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
      })

      println("Spark MLlib K-means clustering test finished.")
  }

  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}

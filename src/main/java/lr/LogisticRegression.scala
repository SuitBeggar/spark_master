package lr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**spark实现逻辑回归算法
 *
 * @description:
 * @Author:bella
 * @Date:2019/12/721:51
 * @Version:
 **/
object LogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegression")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val dataRdd = MLUtils.loadLibSVMFile(sc,"hdfs://master:9000/cf/input/cf.data")

    val dataParts = dataRdd.randomSplit(Array(0.7,0.3),seed = 25L)

    val trainRDD = dataParts(0).cache()

    val testRDD = dataParts(1)

    val LRModel = new LogisticRegressionWithLBFGS().setNumClasses(10).run(trainRDD)

    val prediction = testRDD.map{
      case LabeledPoint(label, features) =>
      val prediction = LRModel.predict(features)
        (prediction,label)
    }

    val  showPrediction = prediction.take(10)

    // 输出测试结果
    println("Prediction" + "\t" + "Label")
    for (i <- 0 to showPrediction.length - 1) {
      println(showPrediction(i)._1 + "\t" + showPrediction(i)._2)
    }

    // 计算误差并输出
    val metrics = new MulticlassMetrics(prediction)
    val precision = metrics.precision
    println("Precision = " + precision)

  }
}

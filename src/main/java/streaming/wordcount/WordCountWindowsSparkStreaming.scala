package streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**SparkStreaming��wordcountʵ��
  * (Windowsʱ�䴰��ģʽͳ�ƣ�Ҳ����ͳ��һ��ʱ���ڵĽ��)
  * Created by fangyitao on 2019/11/19.
  */
object WordCountWindowsSparkStreaming {

    def main(args: Array[String]): Unit = {

        val  conf = new SparkConf().setAppName("WordCountWindowsSparkStreaming")

        val  scc = new StreamingContext(conf,Seconds(10))

        scc.checkpoint("hdfs//master:9000/wordcount/checkpoint")

        val lines = scc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

        val words = lines.flatMap(_.split(" "))

        //ÿ��10��,ͳ��ǰ40����ÿһ�����ʳ��ֵĴ���
        val wordcounts = words.map(x => (x,1)).reduceByKeyAndWindow((v1 : Int,v2 : Int) => v1+v2,Seconds(40),Seconds(10))

        wordcounts.print()

        scc.start()

        scc.awaitTermination()

    }
}

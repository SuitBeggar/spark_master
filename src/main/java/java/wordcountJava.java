/*
package java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

*/
/**
 * @description:
 * @Author:bella
 * @Date:2019/9/2523:05
 * @Version:
 **//*

public class wordcountJava {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountDemon");
        //设置master属性
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD rdd = sc.textFile("hdfs://master:9000/input/1.data");

        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable call(String s) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] arr = s.split(" ");
                for(String ss : arr){
                    list.add(ss) ;
                }
                return list;
            }
        });
        //映射
        JavaPairRDD<String,Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        //聚合
        JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        rdd4.saveAsTextFile("hdfs://master:9000/input/2java.data");
    }
}
*/

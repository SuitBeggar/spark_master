/*
package java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

*/
/**
 * @description:
 * @Author:bella
 * @Date:2019/10/822:43
 * @Version:
 **//*

public class SparkMysql {
    private static Logger logger = LoggerFactory.getLogger(SparkMysql.class);

    public static void main(String[] args){
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));

        SQLContext sqlContext = new SQLContext(sparkContext);

        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://localhost:3306/test";
        //查找的表名
        String table = "user_test";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","123456");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的user_test表内容");

        //读start
        // 读取表中所有数据
        //DataFrame jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
        //显示数据
        jdbcDF.show();

        //读end

        //写start
        //写入的数据内容
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("1 tom 5","2 jack 6","3 alex 7"));
        */
/**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         *//*

        //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(Integer.valueOf(splited[0]),splited[1],Integer.valueOf(splited[2]));
            }
        });

        */
/**
         * 第二步：动态构造DataFrame的元数据。
         *//*

        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        */
/**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         *//*

        DataFrame personsDF = sqlContext.createDataFrame(personsRDD,structType);

        */
/**
         * 第四步：将数据写入到person表中
         *//*

        personsDF.write().mode("append").jdbc(url,"person",connectionProperties);



        //停止SparkContext
        sparkContext.stop();

    }

}
*/

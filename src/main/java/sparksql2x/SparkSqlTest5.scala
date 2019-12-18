package sparksql2x

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description:
 * @Author:suitbeggar
 * @Date:2019/12/19 0:49
 * @Version:
 **/
object SparkSqlTest5 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSqlTest4")
      .getOrCreate()

    import session.implicits._

    val lines = session.createDataset(List("1,zhangsan,china", "2,lisi,usa", "3,wangwu,japan", "4,zhaoliu,England"))

    val td = lines.map(line =>{
        val fileds  = line.split(",")

        val id = fileds(0).toLong
        val name = fileds(1).toString
        val nation = fileds(2).toString

      (id,name,nation)
    })

    val df : DataFrame = td.toDF("id","name","nation")


    val nations = session.createDataset(List("china,zhongguo", "usa,meiguo", "japan,riben", "England,yingguo"))
    val ndataset = nations.map(l => {
      val fields = l.split(",")
      val ename = fields(0).toString
      val cname = fields(1).toString

      (ename, cname)
    })

    val df2 = ndataset.toDF("ename", "cname")

    // method 1
    //    df1.createTempView("t_user")
    //    df2.createTempView("t_nation")
    //    val r = session.sql("select name, cname from t_user join t_nation on t_user.nation == t_nation.ename")


    val r = df.join(df2, $"nation" === $"ename")

    r.show()

    session.stop()
  }
}

package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //TODO  执行逻辑操作

//    val df = spark.read.textFile("datas/user_visit_action.txt")
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show

//    使用df时，如果涉及到转换操作，需要引入转换规则

//    df.select("age","username").show
//    df.select($"age"+1).show
//    df.select('age  + 1).show
//    val seq = Seq(1,2,3,4)
//    val ds  = seq.toDS()
//    ds.show
    val rdd = spark.sparkContext.makeRDD(List((1,"学就",30),(3,"学就1",30)))
    val df = rdd.toDF("id", "name", "age")
    val rowRDD = df.rdd
    val ds = df.as[User]
    val ds1 = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    spark.close()

  }

  case class User(id:Int,name:String,age:Int)
}
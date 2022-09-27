package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //TODO  执行逻辑操作

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("prefixName",(name:String)=>{
      "Name: "+ name
    })
    spark.sql("select age,prefixName(username) from user").show()

    spark.close()



  }

  case class User(id:Int,name:String,age:Int)
}
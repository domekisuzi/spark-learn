package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}

/**
 * 弱类型 查找
 */
object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO  执行逻辑操作
    import spark.implicits._

    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://domekisuzi.fun:3306/shixun")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "shixun")
      .option("password", "123456")
      .option("dbtable", "but")
      .load()


    df.write.format("jdbc")
      .option("url", "jdbc:mysql://domekisuzi.fun:3306/shixun")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "shixun")
      .option("password", "123456")
      .option("dbtable", "but1")
      .mode(SaveMode.Append)
      .save()
    df.show(100)
    spark.close()
  }

}

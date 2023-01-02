package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

/**
 * 弱类型 查找
 */
object Spark03_SparkSQL_UADF1 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO  执行逻辑操作

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    //functions.udaf 强转弱类型
//    spark.udf.register("prefixName",functions.udaf(new MyAvgUDAF()))
//    spark.sql("select age,prefixName(username) from user").show()

    spark.close()

  }

  case class Buff(var total:Long,var count:Long)
  /*
  自定义聚合函数类：计算年龄的平均值
   */
  class MyAvgUDAF extends Aggregator[Long,Buff,Long]{
    //初始值
    //缓冲区的初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total+ a
      b.count = b.count+1
      b
    }

//    合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.count += b2.count
      b1.total += b2.total
      b1
    }

    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total/reduction.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Long] =  Encoders.scalaLong
  }


}

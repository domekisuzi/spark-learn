package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

/**
 * 弱类型 查找
 */
object Spark03_SparkSQL_UADF {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO  执行逻辑操作

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("prefixName",new MyAvgUDAF())
    spark.sql("select age,prefixName(username) from user").show()

    spark.close()

  }


  /*
  自定义聚合函数类：计算年龄的平均值
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction{
    //输入的数据结构
    override def inputSchema: StructType = {
      StructType{
        Array(StructField("age",LongType))
      }
    }

    //缓冲区 数据的结构
    override def bufferSchema: StructType = {
      StructType {
        Array(StructField("total", LongType),StructField("count", LongType))
      }
    }

    //函数计算结构的结构：Out
    override def dataType: DataType = LongType

    //函数的稳定性  没有随机数巴拉的
    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = 0L
//      buffer(1) = 0L
        buffer.update(0,0L)
        buffer.update(1,0L)
    }

    //根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0)+input.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }

    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
    }

    //计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }


}

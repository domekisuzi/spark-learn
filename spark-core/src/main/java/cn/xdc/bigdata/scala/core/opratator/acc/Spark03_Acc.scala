package cn.xdc.bigdata.scala.core.opratator.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //reduce 分区内和分区间计算
    val rdd = sc.makeRDD(List(1, 2, 3, 4) )
    //获取系统累加器
    // spark默认就提供了简单数据的累加器
    val sum = sc.longAccumulator("sum")

    val mapRDD = rdd.map(
      num => {

        sum.add(num)
        num
      }
    )


    mapRDD.collect()
    mapRDD.collect()
    //获取累加器的值
    // 少加：转换算子如果调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子如果调用累加器，如果 行动算子多次执行的话，那么每次使用都会使转换算子调用一次
    // 一般情况下，累加器放入行动算子中进行操作
    println(sum.value)
    sc.stop()
  }

}

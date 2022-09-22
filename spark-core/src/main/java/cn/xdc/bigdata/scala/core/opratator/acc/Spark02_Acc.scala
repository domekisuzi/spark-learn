package cn.xdc.bigdata.scala.core.opratator.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //reduce 分区内和分区间计算
    val rdd = sc.makeRDD(List(1, 2, 3, 4) )
    //获取系统累加器
    // spark默认就提供了简单数据的累加器
    val sum = sc.longAccumulator("sum")
    rdd.foreach(
      num=>{
        sum.add(num)
      }
    )
    println(sum.value)
    sc.stop()
  }
}

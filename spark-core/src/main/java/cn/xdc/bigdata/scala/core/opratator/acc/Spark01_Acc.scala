package cn.xdc.bigdata.scala.core.opratator.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //reduce 分区内和分区间计算
    val rdd = sc.makeRDD(List(1, 2, 3, 4),3)
//    val i = rdd.reduce(_ + _)
//    println(i)
    var sum = 0
    rdd.foreach(
      num=>{
        sum+=num
        println(s"sum = ${sum}  " +Thread.currentThread())
      }
    )

    println(s"sum = ${sum}  " +Thread.currentThread())
    sc.stop()
  }
}

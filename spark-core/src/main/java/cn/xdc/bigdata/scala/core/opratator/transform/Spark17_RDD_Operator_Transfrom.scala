package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * aggregateByKey分区内和分区间运算规则不同的reduceByKey
 */
object Spark17_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val outputPath = "output"
    val file = new File(outputPath)
    if (file.isDirectory) {
      file.listFiles().foreach(
        _.delete()
      )
      file.delete()
    }
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)

    // 第一个为key
    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4),("b",5),("b",7)),2)
    // 第一个参数列表，需要传递一个参数作为初始值， 主要用于碰见第一个key和value进行分区内计算
    // 第二个参数列表 第一个参数为 分区内计算规则，第二个参数为 分区间计算规则
    val reduceRDD = rdd1.aggregateByKey(0)(
      (x,y)=>math.max(x,y)
      ,(x,y)=>x+y
    )

    reduceRDD.collect().foreach(println)
    sc.stop()

  }

}

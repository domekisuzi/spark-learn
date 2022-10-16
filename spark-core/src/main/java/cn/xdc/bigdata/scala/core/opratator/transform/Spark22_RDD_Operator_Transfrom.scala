package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * join  将相同key的所有元素连接在一起 ，相当于内连接
 */
object Spark22_RDD_Operator_Transfrom {
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
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd = sc.makeRDD(List(("a", 5), ("b", 6) ))
    rdd1.leftOuterJoin(rdd).collect().foreach(println)
    rdd1.rightOuterJoin(rdd).collect().foreach(println)
    sc.stop()

  }
}

package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * congroup 将同一组数据相同元素的key的组成一个集合
 */
object Spark23_RDD_Operator_Transfrom {
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
    val rdd = sc.makeRDD(List(("a", 5), ("b", 6) ,("a", 9),("a", 6)))
    rdd1.cogroup(rdd).collect().foreach(println)

    sc.stop()

  }

}

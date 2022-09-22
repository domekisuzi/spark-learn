package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import java.io.File

/**
 * partionBy 分区 ，以及scala一些知识
 */
object Spark15_RDD_Operator_Transfrom {
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

    // reduceByKey 如果key的数据只有一个，是不会参与运算的，不会聚合
    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",1)))

    val reduceRDD = rdd1.reduceByKey((x, y) => {
      x + y
    })

    reduceRDD.collect().foreach(println)
    sc.stop()

  }

}

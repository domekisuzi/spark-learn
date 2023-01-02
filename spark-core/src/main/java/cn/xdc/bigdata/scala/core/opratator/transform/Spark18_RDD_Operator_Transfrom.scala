package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * foldByKey 如果 aggregateByKey 分区内外运算规则一致，则使用这个，相比于reduceByKey多了一个初始值，求平均数的案例
 */
object Spark18_RDD_Operator_Transfrom {
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
    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 5), ("b", 7)), 2)
    // 第一个参数列表，需要传递一个参数作为初始值， 主要用于碰见第一个key和value进行分区内计算
    // 第二个参数列表 第一个参数为 分区内计算规则，第二个参数为 分区间计算规则
    val reduceRDD = rdd1.aggregateByKey((0, 0))(
      (t, v) => {
        println("-----------")
        println(s"t = ${t}", "v:"+v)
        val q =  (t._1 + v, t._2 + 1)
        q
      },(t1, t2) => {
        println("************")
        println(t1,t2)
        val s = (t1._1 + t2._1, t1._2 + t2._2)
        s
      })

    //mapvalues 表示，key保持不变，只对value进行处理  int 值，最后取的值为int
    val resultRDD = reduceRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    resultRDD.collect().foreach(println)
    sc.stop()

  }

}

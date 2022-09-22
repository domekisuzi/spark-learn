package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 扁平化
 */
object Spark04_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    val rdd = sc.makeRDD(List(List(1), 2, 3, 4),2)

    // 1,2,3,4



    val mapRDD = rdd.flatMap {
      case list: List[_] => list
      case s => List(s)
    }
//    val mapRDD = rdd.flatMap(
//      data => {
//        data match {
//          case list: List[_] => list
//          case s => List(s)
//        }
//      }
//    )
    mapRDD .collect().foreach(println)

    sc.stop()
  }
}

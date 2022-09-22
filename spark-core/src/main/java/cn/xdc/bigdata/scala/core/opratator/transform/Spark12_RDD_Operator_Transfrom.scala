package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 排序
 */
object Spark12_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs



//    val rdd = sc.makeRDD(List(7, 2,5,4, 6, 3), 2)
//
//    val newRDD = rdd.sortBy(num =>num)
    val rdd = sc.makeRDD(List(("1",12),("2",12),("3",12),("4",12)), 2)

    //第二个参数为升序或者降序，
    rdd.sortBy(_._1,true).foreach(println)
    sc.stop()
  }
}

package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 扩大分区，提高并行度
 */
object Spark11_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs



    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //coalesce如果需要扩大分区，必去进行shuffle，否则没有意义
    //是spark还提供了简化扩大分区的方法repartition,底层还是使用coalesce
//    val newRDD = rdd.coalesce(3,true)
    val newRDD = rdd.repartition(3)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}

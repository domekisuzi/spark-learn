package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 将分区的数据变为数组
 */
object Spark05_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 1,2,3,4


    //    一个分区的数据形成一个数组
    val glomRDD = rdd.glom()

    //    glomRDD.collect().foreach(data=> println(data.mkString(",")))

   println(glomRDD.map(
      array => array.max
    ).collect().sum)
    sc.stop()
  }
}

package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 交 并 补  拉链
 */
object Spark13_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    // 交集 3,4
    val rdd1 = sc.makeRDD(List((1,2),(3,4)) )

    val rdd2 = sc.makeRDD(List((3,4),(5,6)) )

    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    // 并集  1,2,3,4,3,4,5,6   合并，两个都有
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
    // 差集 1,2
    val rdd5 =  rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))
    //  拉链 【1-3，2-4，3-5，4-6】  ,可以缝合不同的数据类型，但是他们的分区，及元素数需要相同
    val rdd6 =  rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    //第二个参数为升序或者降序，

    sc.stop()
  }
}

package cn.xdc.bigdata.scala.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {

    //根据最大核数创建  [*]
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    val seq = Seq[Int](1, 2, 3, 4)
    //从内存中创建RDD，将内存中集合的数据作为处理的数据源
    //    val rdd = sc.parallelize(seq)
    val rdd = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    sc.stop()
  }
}

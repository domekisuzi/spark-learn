package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 1,2,3,4
    def mapFunction(num:Int): Int ={
        num *2
    }

    val mapRDD = rdd.map(_*2)
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}

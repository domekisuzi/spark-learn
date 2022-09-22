package cn.xdc.bigdata.scala.core.opratator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs
    val rdd = sc.makeRDD(List(1,2,3,4))

    // aggregateBykey 初始值只会参与分区内计算
    // 而 aggregate  初始值会参与分区间的计算
    val result = rdd.fold(10)(_ +_)
    val result1 = rdd.aggregate(10)(_ +_,_ +_)
    println(result)
    sc.stop()

  }
}

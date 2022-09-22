package cn.xdc.bigdata.scala.core.opratator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs
    val rdd = sc.makeRDD(List(1,2,3,4))


    // driver端的内存集合的循环遍历
    rdd.collect().foreach(println)
    println("****************")
    // 其实是Executor端内存数据打印
    // RDD的方法可以将计算逻辑发送到Executor端执行
    // 集合对象的方法都是在同一个节点的内存中完成的
    //  RDD的方法外部操作都是在Driver端执行，而方法内部逻辑代码是在Executor端执行
    rdd.foreach(println)
    sc.stop()

  }
}

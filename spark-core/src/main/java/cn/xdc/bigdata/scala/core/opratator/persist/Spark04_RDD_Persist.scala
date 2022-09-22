package cn.xdc.bigdata.scala.core.opratator.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    val list= List("Hello Scala","Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map{
      println("------------")
      x=>( x,1)
    }

//    mapRDD.cache()
    // checkpoint 需要落盘，需要指定检查点保存路径，
    // 检查点路径保存的文件，当作业执行完后不会删除文件
    // 检查点文件一般放在分布式文件系统中
    mapRDD.checkpoint()
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("-------------------------")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}

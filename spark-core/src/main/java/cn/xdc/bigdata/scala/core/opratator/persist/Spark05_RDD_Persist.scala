package cn.xdc.bigdata.scala.core.opratator.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")

//     cache: 将数据临时存储在内存中进行数据重用（但是数据可能不安全，如内存溢出）
//     persist：将数据临时存储在磁盘文件中进行数据重用 涉及到磁盘IO，性能较低，但是数据安全
//     如果作业执行完毕，临时保存的数据文件就会丢失
//     checkpoint: 将数据长久保存在磁盘文件中进行数据重用，为了保证数据安全，所以一般情况下，会独立执行作业（再执行一次作业），比persist效率更低
//     一般情况下为了提高效率，会和cache联合使用
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    val list= List("Hello Scala","Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map{
      println("------------")
      x=>( x,1)
    }

    mapRDD.cache()
    mapRDD.checkpoint()
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("-------------------------")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}

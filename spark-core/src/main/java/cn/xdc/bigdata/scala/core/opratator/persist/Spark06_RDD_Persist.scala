package cn.xdc.bigdata.scala.core.opratator.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")

//     cache: 将数据临时存储在内存中进行数据重用（但是数据可能不安全，如内存溢出）
//     会在血缘关系中添加新的依赖，一旦出现问题，可以从头读取数据
//     persist：将数据临时存储在磁盘文件中进行数据重用 涉及到磁盘IO，性能较低，但是数据安全
//     如果作业执行完毕，临时保存的数据文件就会丢失
//     checkpoint: 将数据长久保存在磁盘文件中进行数据重用，为了保证数据安全，所以一般情况下，会独立执行作业（再执行一次作业），比persist效率更低
//     一般情况下为了提高效率，会和cache联合使用
//     在执行过程中，会切断血缘关系，重新建立新的血缘关系
//     checkpoint 等同于改变数据源
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    val list= List("Hello Scala","Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map{


      x=> {
       ( x, 1)
      }
    }

//    mapRDD.cache()

    mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("***********")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    println(mapRDD.toDebugString)
    sc.stop()
  }
}

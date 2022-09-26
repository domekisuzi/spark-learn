package cn.xdc.bigdata.scala.core.opratator.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Partitioner {

  def main(args: Array[String]): Unit = {
    println("----------------01------------------")
    val conf = new SparkConf().setMaster("spark://hadoop102:7077").setAppName("RDD")
    println("----------------02------------------")
    val sc: SparkContext = new SparkContext(conf)
    println("----------------0------------------")
    val list = List(("nba", "xxxx"),("cba", "xxxx"),("wnba", "xxxx"),("nba", "xxxx"))
    println("----------------1------------------")
    val rdd = sc.makeRDD(list,3)
    println("----------------2------------------")
    val partRDD = rdd.partitionBy(new MyPartitioner)
    println("----------------3------------------")
    partRDD.saveAsTextFile("output")
    println("----------------4------------------")
    sc.stop()
  }


  /**
   * 自定义分区器
   * 1 继承Partitioner
   * 2 重写方法
   */
  class  MyPartitioner extends Partitioner{
    override def numPartitions: Int = 3


    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "wnba" =>1
        case _=>2
      }
    }
  }
}

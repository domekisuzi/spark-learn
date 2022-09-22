package cn.xdc.bigdata.scala.core.opratator.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Partitioner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(conf)
    val list = List(("nba", "xxxx"),("cba", "xxxx"),("wnba", "xxxx"),("nba", "xxxx"))
    val rdd = sc.makeRDD(list,3)
    val partRDD = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")
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

package cn.xdc.bigdata.scala.core.opratator.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Partitioner {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis();
    println("----------------01------------------")
    val conf = new SparkConf().setMaster("spark://hadoop102:7077").setAppName("RDD")
    val stage1 = System.currentTimeMillis();
    println("创建conf用时 "+(stage1-startTime)/1000)
    println("----------------02------------------")
    val sc: SparkContext = new SparkContext(conf)
    val stage2 = System.currentTimeMillis();
    println("创建context用时 "+(stage2-stage1)/1000)
    println("----------------0------------------")
    val list = List(("nba", "xxxx"),("cba", "xxxx"),("wnba", "xxxx"),("nba", "xxxx"))
    val stage3 = System.currentTimeMillis();
    println("创建list用时 "+(stage3-stage2)/1000)
    println("----------------1------------------")
    val rdd = sc.makeRDD(list,3)
    val stage4 = System.currentTimeMillis();
    println("创建rdd用时 "+(stage4-stage3)/1000)
    println("----------------2------------------")
    val partRDD = rdd.partitionBy(new MyPartitioner)
    val stage5 = System.currentTimeMillis();
    println("创建rdd分区用时 "+(stage5-stage4)/1000)
    println("----------------3------------------")
    partRDD.saveAsTextFile("output")
    val stage6 = System.currentTimeMillis();
    println("算子执行用时 "+(stage6-stage5)/1000)
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




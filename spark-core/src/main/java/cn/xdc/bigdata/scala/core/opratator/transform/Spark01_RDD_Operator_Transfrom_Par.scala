package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 *  map
 */
object Spark01_RDD_Operator_Transfrom_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    // 1,2,3,4
    def mapFunction(num: Int): Int = {
      num * 2
    }

    //并行执行，但是如果在分区内则是串行执行
    //只有前面的数据执行完，后面的数据才会执行
    //不同分区数据计算是无序的
    //来一条处理一条
    val mapRDD = rdd.map((num: Int) => {
      println("---------"+num)
      num
    })
    val mapRDD1 = mapRDD.map(num => {
      println("=========" + num)
      num
    }
    )
    mapRDD1.collect().foreach(println)

    sc.stop()
  }
}

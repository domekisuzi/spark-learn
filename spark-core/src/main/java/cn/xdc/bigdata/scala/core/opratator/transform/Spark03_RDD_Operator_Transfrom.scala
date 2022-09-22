package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 给定index分区
 */
object Spark03_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    // 1,2,3,4


    //并行执行，但是如果在分区内则是串行执行
    //只有前面的数据执行完，后面的数据才会执行
    //不同分区数据计算是无序的
    //来一条处理一条
    val mapRDD = rdd.mapPartitionsWithIndex(
      (index,iter)=>{
          if (index ==1 ) {
            List(iter.max).iterator
          }
          else {
            //Nil为返回空
            Nil.iterator
          }
      })
    mapRDD .collect().foreach(println)

    sc.stop()
  }
}

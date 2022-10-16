package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 提高效率，先计算分区再运算 缓冲    map 高阶 mapPartitions
 */
object Spark02_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    // 1,2,3,4
    //分区的作用是，先分好区，把所有分区的数据加载到内存,相比于传统map，优点为，map一条一条处理，而这个是分区好处理，起到缓冲作用
    //但是如果内存过小，数据量过大，在处理时会持有数据的引用， 无法释放，则会存在内存溢出的情况
    val mapRDD = rdd.mapPartitions(iter=>{
      iter.map(_*2)
    })
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}

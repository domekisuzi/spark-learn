package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 缩减分区 ，用于大数据集过滤后，提高小数据集的执行效率
 */
object Spark10_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs


    //去重，scala底层为hashset，rdd为 map(_=>(_,null)).reduceBykey((x,_)=>x, partitionersNum).map(_._1) ，使用reduce聚合后去重
    val rdd = sc.makeRDD(List(1, 2, 3, 4 ,5,6), 3)

    // coalesce 方法默认不会将数据打乱重新组合，1，2 ， 3，4， 5，6， 分区 ，不会 分出 1，2，3  ， 因为3，4被打乱了。
    // 这会导致数据不均衡，需要使用shuffle,将后面参数设置为true
    val newRDD = rdd.coalesce(2,true)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}

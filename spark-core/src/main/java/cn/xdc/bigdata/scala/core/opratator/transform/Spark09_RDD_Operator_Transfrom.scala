package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 去重
 */
object Spark09_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs


    //去重，scala底层为hashset，rdd为 map(_=>(_,null)).reduceBykey((x,_)=>x, partitionersNum).map(_._1) ，使用reduce聚合后去重
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4), 1)


    rdd.distinct().collect().foreach(println)

    sc.stop()
  }
}

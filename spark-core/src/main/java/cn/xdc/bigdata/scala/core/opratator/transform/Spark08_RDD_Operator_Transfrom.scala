package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 随机
 */
object Spark08_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs




    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),1)
    //第一个为是否放回，true为放回，第二个为：不放回每条数据被抽取的概率，放回每个数据被抽取的可能次数，第三个为随机数种子,如果没写则根据系统时间来给
    //用于数据倾斜的情况
    println(rdd.sample(withReplacement = true, 0.4, 1).collect().mkString(","))
    sc.stop()
  }
}

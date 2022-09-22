package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 过滤
 */
object Spark07_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs



    //分区和分组没有必然关系,groupby会将数据打乱,重新组合,这个操作我们称之为shuffle,把分组内容放入同一个分区 
    val rdd = sc.makeRDD(List(1,2,3,4),1)

    rdd.filter(_%2==0).collect().foreach(println)


    sc.stop()
  }
}

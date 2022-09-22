package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import java.io.File

/**
 * partionBy 分区 ，以及scala一些知识
 */
object Spark14_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val outputPath = "output"
    val file = new File(outputPath)
    if (file.isDirectory) {
      file.listFiles().foreach(
        _.delete()
      )
      file.delete()
    }
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    // 交集 3,4
    val rdd1 = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD = rdd1.map((_,1))
    // 本来rdd并没有 partitionBy这个方法  但是rdd 的伴生对象中有这个方法，而且rdd 有个隐式转换的方法，将自己转化成了伴生对象
    // 经过二次编译，rdd可以直接调用这个方法

    // hashpartitioner  会对分区器内的 规则进行判断，如果两次规则相等则不会重新将数据重组，可以自己写分区器进行分区

    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile(outputPath)

    sc.stop()
  }
}

package test1

import cn.xdc.bigdata.scala.core.test1
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //建立链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //读取文件,获取每行的数据
    val line = sc.textFile("datas")
    //添加依赖，
    // 在 scala中可以用_代替参数，相当于 x => x.split(" ")
    //flatmap返回的任仍然是集合，此处把他们都分割开了
    val words = line.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    //spark提供了更多的功能，可以将分组和聚合一起使用
    val wordGroup = wordToOne.groupBy(t => t._1)
    //    val wordToCount = wordToOne.reduceByKey((x, y) => {
    //      x + y
    //    })
    val wordToCount = wordToOne.reduceByKey(_ + _)
    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }
}

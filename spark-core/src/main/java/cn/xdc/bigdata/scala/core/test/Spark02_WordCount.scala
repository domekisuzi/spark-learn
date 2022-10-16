package test1

import cn.xdc.bigdata.scala.core.test1
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //建立链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //读取文件,获取每行的数据
    val line = sc.textFile("datas")
    //添加依赖，
    // 在 scala中可以用_代替参数，相当于 x => x.split(" ")
    //flatmap返回的任仍然是集合，此处把他们都分割开了
    val value = line.map(_.split(','))
    val value1 = line.filter((x: String) => x.split(' ')(0) != "asd")
    val words = line.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordGroup = wordToOne.groupBy(t => t._1)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            //t1,t2 表示从左到右，同reduceLeft
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }
    val value2 = wordToCount.filter(_._1 != "2")
    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }
}

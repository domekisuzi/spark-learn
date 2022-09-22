package cn.xdc.bigdata.scala.core.opratator.action


import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * wordcount的不同实现方式
 */
object Spark04_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    wordcount1(sc)
    sc.stop()
  }

  def wordcount1(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group = words.groupBy(
      word => word
    )
    val wordCount = group.mapValues(iter => iter.size)
  }

  //groupByKey
  def wordcount2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val group = wordOne.groupByKey()
    val wordCount = group.mapValues(iter => iter.size)
  }

  //reduceByKey
  def wordcount3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.reduceByKey(_ + _)

  }

  //foldByKey
  def wordcount4(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.foldByKey(0)(_ + _)

  }

  //aggregate
  def wordcount5(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.aggregate(0)(
      (x:Int,y)
      =>
      x + y._2,
      _ + _
    )
  }

  //combineByKey
  def wordcount6(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)
  }

  //countByKey
  def wordcount7(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.countByKey()
  }

  //countByValue
  def wordcount8(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    val wordCount = words.countByValue()
  }

  //reduce
  def wordcount9(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    //    val mapWord = words.map((x:String)=>(x,1))
    val mapWord = words.map {
      word => {
        mutable.Map((word, 1L))
      }
    }
    val wordCount = mapWord.reduce(
      (map1, map2)
      => {
        map2.foreach {
          case (word, count) => {
            //OL:??
            val newCount  = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }

    )
    println(wordCount)

  }


}

package cn.xdc.bigdata.scala.homework

import cn.xdc.bigdata.scala.core.opratator.acc.Spark04_Acc.MyAccumulator
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Work1_Deduplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.textFile("input")

    val sum = new MyAccumulator
    sc.register(sum)
    rdd.foreach({
      (x: String) => {
        val temp = x.split(",")
        val s = (temp(0), temp(1))
        sum.add(s)
      }
    })
    val res= sum.value.toList.sortBy(_._1)
    res.foreach(println)
    sc.makeRDD(res,1).saveAsHadoopFile("output",classOf[String],classOf[String],classOf[myMultipleTextOutputFormat])
    sc.stop()
  }


  class MyAccumulator() extends AccumulatorV2[(String, String), mutable.Set[(String, String)]] {

    private var mySet = mutable.Set[(String, String)]()


    // 判断是否为初始状态
    override def isZero: Boolean = mySet.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Set[(String, String)]] = new MyAccumulator()

    override def reset(): Unit = mySet.clear()

    override def add(v: (String, String)): Unit = {
      mySet.add(v)
    }

    //Driver 合并累加器
    override def merge(other: AccumulatorV2[(String, String), mutable.Set[(String, String)]]): Unit = {
      //      this.mySet  +=other.value
      other.value.foreach(
        (x: (String, String)) => this.mySet.add(x)
      )
    }

    override def value: mutable.Set[(String, String)] = mySet
  }

  class  myMultipleTextOutputFormat extends  MultipleTextOutputFormat[Any,Any]{
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      println("----------")
      println(key.toString +":" + value.toString+":"+name.toString)
      println("----------")
//      key.toString
      "C"
    }
  }
}


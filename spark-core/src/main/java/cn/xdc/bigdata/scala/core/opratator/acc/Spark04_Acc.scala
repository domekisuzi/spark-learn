package cn.xdc.bigdata.scala.core.opratator.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //reduce 分区内和分区间计算
    val rdd = sc.makeRDD(List("AADAF das","asdads das ") )
    //获取系统累加器
    // spark默认就提供了简单数据的累加器

    val sum = new MyAccumulator
    sc.register(sum)
    val mapRDD = rdd.foreach(
      num => {
        sum.add(num)
        num
      }
    )
    //获取累加器的值
    // 少加：转换算子如果调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子如果调用累加器，如果 行动算子多次执行的话，那么每次使用都会使转换算子调用一次
    // 一般情况下，累加器放入行动算子中进行操作
    println(sum.value)
    sc.stop()
  }

  /**
   * 重写累加器步骤
   * 1.继承Acculator2，定义泛型
   * 2.重写方法
   */
  class MyAccumulator() extends AccumulatorV2[String,mutable.Map[String,Long]]{

    private var wcMap = mutable.Map[String,Long]()


    // 判断是否为初始状态
    override def isZero: Boolean = wcMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

    override def reset(): Unit = wcMap.clear()

    override def add(v: String): Unit = {
        val newCnt = wcMap.getOrElse(v,0L) + 1
        wcMap.update(v,newCnt)
    }

    //Driver 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach{
        case(word,count)=>{
          val newCount =  map1.getOrElse(word,0L) +  count
          map1.update(word,newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = wcMap
  }
}

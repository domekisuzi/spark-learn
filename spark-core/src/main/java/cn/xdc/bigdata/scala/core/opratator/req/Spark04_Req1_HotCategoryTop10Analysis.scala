package cn.xdc.bigdata.scala.core.opratator.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Q：有大量shuffle操作
 */
object Spark04_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    //    actionRDD重复使用过多
    val actionRDD = sc.textFile("datas/user_visit_action.txt")
    val acc = new HotCategoryAccumulator()

    sc.register(acc, "hotCategory")

    val flatRDD = actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-") {
          acc.add((datas(6), "click"))
        }
        else if (datas(8) != null) {
          val ids = datas(8).split(",")

          ids.foreach(id => acc.add((id, "order")))

        } else if (datas(8) != null) {
          val ids = datas(10).split(",")
          ids.foreach(id => acc.add((id, "pay")))

        }
        else {
          Nil
        }
      }
    )

    val accVal = acc.value
    val categories = accVal.map(_._2)

    val sort = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.clickCnt) {
            true
          }
          else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          }
          else {
            false
          }
        } else {
          false
        }
      }
    )
    //    resultRDD.sortBy(_._2,false).take(10).foreach(println)
    sort.take(10).foreach(println)
    sc.stop()
  }

  case class HotCategory(var cid: String, var clickCnt: Int, var orderCnt: Int,var  payCnt: Int) {

  }

  /**
   * 自定义累加器
   * 1.继承AccumulatorV2，定义泛型
   * IN: (品类ID，行为类型)
   * OUT：muatble.Map[String,HotCategory]
   * 2.重写方法
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hcMap.isEmpty


    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator()

    override def reset(): Unit = hcMap.clear()

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._1
      val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      }
      else if (actionType == "pay") {
        category.payCnt += 1
      }

      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value
      map2.foreach {
        case (cid, hc) => {
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}

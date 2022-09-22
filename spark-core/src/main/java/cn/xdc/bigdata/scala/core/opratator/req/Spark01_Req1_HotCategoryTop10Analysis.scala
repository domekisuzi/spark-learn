package cn.xdc.bigdata.scala.core.opratator.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    val  orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cids = datas(8)
        cids.split(",").map(id => (id, 1))
      }
    ).reduceByKey(_ + _)


    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cids = datas(10)
        cids.split(",").map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    val cogroupRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt, orderCnt, payCnt = 0
        val iter1 = clickIter.iterator
        val iter2 = orderIter.iterator
        val iter3 = payIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)

      }
    }
    analysisRDD.sortBy(_._2).take(10).foreach(println)
    sc.stop()
  }
}

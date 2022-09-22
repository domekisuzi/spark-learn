package cn.xdc.bigdata.scala.core.opratator.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


//    actionRDD重复使用过多
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

    val rdd1 = payCountRDD.map(x => (x._1, (0, 0, x._2)))
    val rdd2  = orderCountRDD.map(x => (x._1, (0, x._2,0 )))
    val rdd3 = clickCountRDD.map(x => (x._1, (x._2, 0, 0)))
    val sourceRDD = rdd1.union(rdd2).union(rdd3)
    val analysisRDD = sourceRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })
    analysisRDD.sortBy(_._2,false).take(10).foreach(println)
//    cogroup 有可能使用shuffle
//    val cogroupRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
//
//    val analysisRDD = cogroupRDD.mapValues {
//      case (clickIter, orderIter, payIter) => {
//        var clickCnt, orderCnt, payCnt = 0
//        val iter1 = clickIter.iterator
//        val iter2 = orderIter.iterator
//        val iter3 = payIter.iterator
//        if (iter1.hasNext) {
//          clickCnt = iter1.next()
//        }
//        if (iter2.hasNext) {
//          orderCnt = iter2.next()
//        }
//        if (iter3.hasNext) {
//          payCnt = iter3.next()
//        }
//
//        (clickCnt, orderCnt, payCnt)
//
//      }
//    }
//    analysisRDD.sortBy(_._2).take(10).foreach(println)

    sc.stop()
  }
}

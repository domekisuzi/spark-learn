package cn.xdc.bigdata.scala.core.opratator.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)

    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )

    actionDataRDD.cache()
    //    计算分母

    val ids = List(1, 2, 3, 4, 5, 6, 7)
    val okflowIds  = ids.zip(ids.tail)
    val pargIdToCountMap = actionDataRDD.filter(
      action=>{
        //init 去除最后一个
        ids.init.contains(action.page_id)
      }
    ).map(
      action =>
      {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    //    计算分子
    val sessionRDD = actionDataRDD.groupBy(_.session_id)
    val mvRDD = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
        //        将 [1,2,3,4] 和 [2,3,4] 连接
        val pageflowIds = flowIds.filter(
          t=>okflowIds.contains(t)
        ).zip(flowIds.tail)
        pageflowIds.map(
          t => {
            (t, 1)
          }
        )
      }
    )
    val tmp1 = mvRDD.map(_._2)

    val flatRDD = tmp1.flatMap(list => list)

    //    println("flatRDD:--------------------")
    //    flatRDD.collect().foreach(println)
    val dataRDD = flatRDD.reduceByKey(_ + _)
    dataRDD.foreach {
      case ((id1: Long, id2: Long), sum) => {
        val lon = pargIdToCountMap.getOrElse(id1, 0L)
        println(s"单挑转换率为${id1}=>${id2}:" + sum.toDouble / lon)
      }
    }
    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long
                            ) //城市 id
}

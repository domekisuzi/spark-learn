package cn.xdc.bigdata.scala.core.opratator.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Q：有大量shuffle操作
 */
object Spark03_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


//    actionRDD重复使用过多
    val actionRDD = sc.textFile("datas/user_visit_action.txt")
    val flatRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-") {
          List((datas(6), (0, 0, 0)))
        }
        else if (datas(8) != null) {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(8) != null) {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        }
        else {
          Nil
        }
      }
    )

    val resultRDD = flatRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    resultRDD.sortBy(_._2,false).take(10).foreach(println)

    sc.stop()
  }
}

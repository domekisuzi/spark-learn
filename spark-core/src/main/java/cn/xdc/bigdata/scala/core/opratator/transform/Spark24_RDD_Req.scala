package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 *  案例实操
 */
object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val outputPath = "output"
    val file = new File(outputPath)
    if (file.isDirectory) {
      file.listFiles().foreach(
        _.delete()
      )
      file.delete()
    }
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.textFile("datas/agent.log")

    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    val newMapRDD = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }
    val groupRDD = newMapRDD.groupByKey()
    groupRDD.mapValues(
      iter=>{
//        变成可排序的集合
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    ).collect().foreach(println)


    sc.stop()

  }

}

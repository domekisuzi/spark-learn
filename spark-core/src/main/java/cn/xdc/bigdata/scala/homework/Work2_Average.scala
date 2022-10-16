package cn.xdc.bigdata.scala.homework

import org.apache.spark.{SparkConf, SparkContext}

object Work2_Average {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.textFile("input").map {
      x => {
        val tmp = x.split(' ')
        (tmp(0),(tmp(1).toFloat,1L))
      }
    }
    rdd.reduceByKey((x,y)=>{
      ( x._1+y._1,x._2+y._2)
    }).map(x=>(x._1,(x._2._1/x._2._2).formatted("%.2f"))).sortBy(_._2,true).saveAsTextFile("output")

    sc.stop()
  }
}

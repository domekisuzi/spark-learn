package cn.xdc.bigdata.scala.core.opratator.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //reduce 分区内和分区间计算
    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)) )
    //获取系统累加器
    // spark默认就提供了简单数据的累加器
//    val rdd2 = sc.makeRDD(List(("a","4"),("b","5"),("c","6")) )

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // join会导致数据量集合增长并且会影响shuffle的新能，不推荐系统
//    val joinRDD = rdd.join(rdd2)
//    joinRDD.collect().foreach(println)
//
//    通过这种方式，我们避免了shuffle，提高了性能
//    闭包数据，都是以task为单位发送的，每个任务中包含闭包数据， 这样可能会导致，
//        一个Executor中含有大量重复的数据，并且占用大量的内存
//    Executor 其实就是一个JVM，所以在启动时，会自动分配内存，
//        完全可以将任务中的闭包数据放置在Executor的内存中，达到共享的目的
//    spark中的广播变量就可以将闭包数据保存到Executor的内存中（分布式共享只读变量）
    val bc = sc.broadcast(map)



    rdd.map{

      case (w,c)=>{
        println(Thread.currentThread().getName)

        val l:Int = bc.value.getOrElse(w,0)
        (w,(c, l))
      }
    }.collect().foreach(println)
    sc.stop()
  }

}

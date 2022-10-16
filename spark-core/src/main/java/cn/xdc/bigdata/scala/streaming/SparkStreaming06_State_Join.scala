package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 之前值累加
 */
object SparkStreaming06_State_Join {
  def main(args: Array[String]): Unit = {
    //需要传递两个参数1；环境配置 2：采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(7))

    val data9999 = ssc.socketTextStream("localhost", 9999)
    val data8888 = ssc.socketTextStream("localhost", 8888)

    val map9999 = data9999.map((_, 1))
    val map8888 = data8888.map((_, 1))
//    底层是rdd的join
    val joinDS = map9999.join(map8888)
    joinDS.print()
    ssc.start()
    ssc.awaitTermination()

    // transform方法可以将底层RDD获取到后进行操作
    // 1. DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code : Driver端

  }
}

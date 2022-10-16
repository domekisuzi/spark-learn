package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 之前值累加
 */
object SparkStreaming06_State_Window {
  def main(args: Array[String]): Unit = {
    //需要传递两个参数1；环境配置 2：采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)

    val wordToOne = lines.map((_, 1))
    // Code : Driver端
    //设置滑动步长大于采集时长可避免重复数据
    val wordToCount = wordToOne.window(Seconds(6),Seconds(6)).reduceByKey(_ + _)
    wordToCount.print()
    ssc.start()
    ssc.awaitTermination()

    // transform方法可以将底层RDD获取到后进行操作
    // 1. DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code : Driver端

  }
}

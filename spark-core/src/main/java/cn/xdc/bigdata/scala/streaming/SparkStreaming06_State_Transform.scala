package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 之前值累加
 */
object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {
    //需要传递两个参数1；环境配置 2：采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)
    val newDS: DStream[String] = lines.transform(
      rdd => {
        // Code : Driver端，（周期性执行）
        rdd.map(
          str => {
            // Code : Executor端
            str
          }
        )
      }
    )
    // Code : Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        // Code : Executor端
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()

    // transform方法可以将底层RDD获取到后进行操作
    // 1. DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code : Driver端

  }
}

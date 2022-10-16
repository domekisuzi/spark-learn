package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * 回复数据
 */
object SparkStreaming09_State_Resume {
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      ssc.checkpoint("cp")
      val lines = ssc.socketTextStream("localhost", 9999)

      val wordToOne = lines.map((_, 1))
      ssc
    })
    ssc.checkpoint("cp")

    //需要传递两个参数1；环境配置 2：采集周期

    ssc.start()
    ssc.awaitTermination()

  }
}

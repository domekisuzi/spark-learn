package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //需要传递两个参数1；环境配置 2：采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map((_, 1))
    val wordToCount = wordToOne.reduceByKey(_ + _)
    wordToCount.print()
    //    由于spark采集器是长期执行的任务，所以不能直接关闭，不能让main方法执行完毕
    //    1.启动采集器2.等待采集器的关闭
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}

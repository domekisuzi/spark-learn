package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 之前值累加
 */
object SparkStreaming07_State_OutPut {
  def main(args: Array[String]): Unit = {
    //需要传递两个参数1；环境配置 2：采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cpp")
    val lines = ssc.socketTextStream("localhost", 9999)

    val wordToOne = lines.map((_, 1))
    //当窗口范围比较大，但是滑动幅度比较小，可以采取增加数据的方式，无需重复计算
    val windowDS = wordToOne.reduceByKeyAndWindow((x: Int, y: Int) => x + y, (x: Int, y: Int) => {
      x - y
    }, Seconds(9), Seconds(3))
    //如果没有输出，则会报错
//    windowDS.print()

    ssc.start()
    ssc.awaitTermination()

    // transform方法可以将底层RDD获取到后进行操作
    // 1. DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code : Driver端

  }
}

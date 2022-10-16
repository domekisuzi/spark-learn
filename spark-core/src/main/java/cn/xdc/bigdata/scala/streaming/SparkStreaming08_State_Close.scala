package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * 优雅的关闭
 */
object SparkStreaming08_State_Close {
  def main(args: Array[String]): Unit = {
    //需要传递两个参数1；环境配置 2：采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cpp")
    val lines = ssc.socketTextStream("localhost", 9999)

    val wordToOne = lines.map((_, 1))


    ssc.start()
    ssc.awaitTermination()
//    优雅的关闭，将当前数据处理完毕后再关闭
    new Thread(()=>{
      while (true){
        if(ssc.getState() == StreamingContextState.ACTIVE){
          ssc.stop(true,true)
          System.exit(0)
        }
      }
    }).start()

    // Code : Driver端

  }
}

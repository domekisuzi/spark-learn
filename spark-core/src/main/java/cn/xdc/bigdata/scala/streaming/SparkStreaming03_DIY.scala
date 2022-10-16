package cn.xdc.bigdata.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //3.创建 RDD 队列
    val messageDS = ssc.receiverStream(new MyReceiver())
    messageDS.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /*
  自定义数据采集器
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY ){
    private var flg = true
    override def onStart(): Unit = {
      new Thread(()=>{
        while(flg){
          val message  = "采集的数据为:"+  new Random().nextInt(10).toString
          store(message)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flg = false
    }
  }
}

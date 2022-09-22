package cn.xdc.bigdata.scala.core.opratator.framwork.common

import cn.xdc.bigdata.scala.core.opratator.framwork.controller.WordCountController
import cn.xdc.bigdata.scala.core.opratator.framwork.util.EnvUtil.{clear, put}
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  /**
   * 控制抽象，可以把一段代码传进来执行,柯里化
   * @param op
   */

  def start(master:String="local[*]",app:String="Application")(op: =>Unit): Unit ={
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    //读取文件,获取每行的数据
    put(sc)
    try {
      op
    }catch {
      case ex=>println(ex.getMessage)
    }
    sc.stop()
    clear()
  }
}

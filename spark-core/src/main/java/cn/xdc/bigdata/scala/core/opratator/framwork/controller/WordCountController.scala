package cn.xdc.bigdata.scala.core.opratator.framwork.controller


import cn.xdc.bigdata.scala.core.opratator.framwork.common.TController
import cn.xdc.bigdata.scala.core.opratator.framwork.service.WordCountService

class WordCountController  extends  TController{
  private val  wordCountService = new WordCountService()
  def dispatch(): Unit ={
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}

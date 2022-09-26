package cn.xdc.bigdata.scala.core.opratator.framwork.application

import cn.xdc.bigdata.scala.core.opratator.framwork.common.TApplication
import cn.xdc.bigdata.scala.core.opratator.framwork.controller.WordCountController

object WordCountApplication extends App  with  TApplication{
  //建立链接
  start(){

    val controller = new WordCountController()
    controller.dispatch()

  }


}

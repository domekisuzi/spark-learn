package cn.xdc.bigdata.scala.core.opratator.framwork.common

import cn.xdc.bigdata.scala.core.opratator.framwork.util.EnvUtil.take

trait TDao {
  def readFile(path: String) = {
    take().textFile("datas/word.txt")
  }
}

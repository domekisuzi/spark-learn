package cn.xdc.bigdata.scala.core.opratator.framwork.service


import cn.xdc.bigdata.scala.core.opratator.framwork.common.TService
import cn.xdc.bigdata.scala.core.opratator.framwork.dao.WordCountDao

class WordCountService extends TService{
  private val wordCountDao = new WordCountDao()


  def dataAnalysis() ={

    val line = wordCountDao.readFile("datas/word.txt")
    //添加依赖，
    // 在 scala中可以用_代替参数，相当于 x => x.split(" ")
    //flatmap返回的任仍然是集合，此处把他们都分割开了
    val words = line.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    //spark提供了更多的功能，可以将分组和聚合一起使用
    val wordGroup = wordToOne.groupBy(t => t._1)
    //    val wordToCount = wordToOne.reduceByKey((x, y) => {
    //      x + y
    //    })
    val wordToCount = wordToOne.reduceByKey(_ + _)
    val array = wordToCount.collect()
    array.foreach(println)
    array
  }
}

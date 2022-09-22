package test1

import cn.xdc.bigdata.scala.core.test1

object test {
  def main(args: Array[String]): Unit = {

    //groupby操作
    //    val list1 = List("刘德华" -> "男", "刘亦菲" -> "女", "胡歌" -> "男")
    //    //2. 请按照性别进行分组.
    //    //val list2 = list1.groupBy(x => x._2)
    //    //简写形式
    //    val list2 = list1.groupBy(_._2)
    //    println(list2)
    //    val list3 = list2.map(x => x._1 -> x._2.size)
    //    println(s"list3: ${list3}")

    //1. 定义一个列表，包含以下元素：1,2,3,4,5,6,7,8,9,10
    val list1 = (1 to 10).toList
    //2. 使用reduce计算所有元素的和
    //val list2 = list1.reduce((x, y) => x + y)
    //简写形式:
    val list2 = list1.reduce(_ + _)
    val list3 = list1.reduceLeft(_ + _)
    val list4 = list1.reduceRight(_ + _)
    println(s"list2: ${list2}")
    println(s"list3: ${list3}")
    println(s"list4: ${list4}")

  }
}

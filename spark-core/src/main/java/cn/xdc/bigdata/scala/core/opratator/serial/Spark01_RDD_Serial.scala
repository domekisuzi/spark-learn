package cn.xdc.bigdata.scala.core.opratator.serial

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new
        SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个 RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark",
      "hive", "atguigu"))
    //3.1 创建一个 Search 对象
    val search = new Search("hello")
    search.getMatch1(rdd).collect().foreach(println)
    sc.stop()

  }

  //类的构造参数其实是类的属性,构造参数需要进行闭包检测，等同于 类进行闭包检测

  class Search(query:String ) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(this.isMatch)
      rdd.filter(isMatch)
  }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(x => x.contains(this.query))
//      rdd.filter(x => x.contains(query))
      val q = query
      rdd.filter(x => x.contains(q))
    }
  }

}

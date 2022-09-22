package test1

import cn.xdc.bigdata.scala.core.test1
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
        //建立链接
//            val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
//        val sc = new SparkContext(sparkConf)
//        //读取文件,获取每行的数据
//        val line = sc.textFile("datas")
//        //添加依赖，
//        // 在 scala中可以用_代替参数，相当于 x => x.split(" ")
//
//        //flatmap返回的任仍然是集合，此处把他们都分割开了
//        val words = line.flatMap(_.split(" "))
//        val group = words.groupBy(word => word)
//
//
//        val wordToCount = group.map {
//          case (word, list) => {
//           ( word, list.size)
//          }
//        }
//        val array = wordToCount.collect()
//        array.foreach(println)
//        sc.stop()

  }
  def  forI(){

  }

}





//构造器中带val,var的是共有方法,不带的为私有
//默认值参数,在java中是必须填的
class s(val d: String, val q: String, val b: String = "asd", c: String = "a") extends interface[String] {
  //私有属性
  private var _s = 0
  //公有属性,默认带public
  val bbb = 0

  //get方法
  def s = _s

  //set方法,需要在名getter方法的后面加_=
  def s_=(test: Int): Unit = {
    _s = test
  }

  def yuanzu(): Unit = {
    println(s"q = ${q}")
    //我们通过这种方式来访问元祖中的元素，_n代表第n个原色
    println(ingredient._1)
    //我们通过这种方式将元祖结构，单个取出其中的元素，相当于创建了str和number
    val (str, number) = ingredient

  }

  def advancedFunction(): Unit ={
    val sa =  Seq(2000,3,21)
    val double  = (x:Int) => x * 2
    val seq = sa.map(double)
    sa.map(_ * 2)

  }


  def next() = "asd"

  def forEachTest = {
    //相当于，Switch case

    ingredient match {
      case ("1", d) => println(d)
      case (a, b) => println(a, b)
      case s  if (s._1 =="1") => println("可以加个if来对case条件进行限制")
      case _ => println("这是default的表示方法")
    }
    //结构
    val numPairs = List((2, 5), (3, -7), (20, 56))
    for ((a, b) <- numPairs) {
      println(a * b)
    }


  }

  //高阶函数，可以接受一个函数为参数
  def promotion(s:List[Double], promotionFunction:Double=>Double): Unit ={

  }

  //定义private[this]，使得它的属性只能本类使用，甚至伴生类都无法访问
  class c(private var tttt: String, private[this] val q: String)

  override val name: String = "_12"
  override val test: String = "_123"
  //scala 允许嵌套方法
  def qiantao(): Int = {
    def qiantao1() = 1
    qiantao1()
  }


  //返回函数的函数，可以用来配置
  def urlBuilder(ssl: Boolean, domainName: String): (String, String) => String = {
    val schema = if (ssl) "https://" else "http://"
    (endpoint: String, query: String) => s"$schema$domainName/$endpoint?$query"
  }

  val domainName = "www.example.com"
  def getURL = urlBuilder(ssl=true, domainName)
  val endpoint = "users"
  val query = "id=1"
  val url = getURL(endpoint, query)
}

//这是scala的一个, 接口,可以接泛型
trait interface[A] {
  val name: String
  val test: A
  val ingredient = ("洗吧", 123): (String, Int)
}
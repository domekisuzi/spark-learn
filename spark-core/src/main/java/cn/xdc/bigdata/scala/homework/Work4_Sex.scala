package cn.xdc.bigdata.scala.homework

import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream, PrintWriter}
import scala.util.Random

object Work4_Sex {
  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("class").setMaster("local[*]")
//    //2.创建 SparkContext，该对象是提交 Spark App 的入口
//    val sc: SparkContext = new SparkContext(conf)
    var i = 1
    val data = new   Array[Human](1000)
    for(i <- 1 to 1000){
     data(i-1) =  Human(i,gender = randomGender, height = randomHeight)
    }
    val  out  = new PrintWriter("create/peopleinfo.txt")



//    sc.stop()
    for (t:Human<-data)  out.println(t.toString)
    out.close()
  }

  object  Sex extends Enumeration {
    val MAN = "M"
    val FEMALE = "F"
  }
  case class Human(id:Int,gender:String,height:Int){
    override def toString: String = id+"  "+gender+"  "+height
  }

  def randomHeight=  Random.nextInt(50)+140
  def randomGender  = if(Random.nextInt(2) ==1){
    Sex.MAN
  }else{
    Sex.FEMALE
  }
}

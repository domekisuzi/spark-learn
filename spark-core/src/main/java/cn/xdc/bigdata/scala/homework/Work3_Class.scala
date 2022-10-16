package cn.xdc.bigdata.scala.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Work3_Class {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("class").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.textFile("banji").filter(x=>x.split(" ").length == 6)
    val studentRDD = rdd.map {
      x => {
        val tmp = x.split(" ")
        val data = Student(tmp(0), tmp(1), tmp(2).toLong, tmp(3), tmp(4), tmp(5).toLong)
        data
      }
    }

    work12(studentRDD)
    sc.stop()
  }

  def work1( studentRDD:RDD[Student]): Unit ={
    val students = studentRDD.filter(_.age > 20).collect()
    students.foreach(println)
    val set = mutable.Set[String]()

    students.foreach { x => {
      set.add(x.name)
    }
    }
    println("有"+set.size+"个20岁以上同学参加考试")
  }

  def work2(studentRDD:RDD[Student]): Unit = {
    val students = studentRDD.filter(_.sex == "女").collect()
    val set  =   mutable.Set[String]()

    students.foreach{ x=> {
      set.add(x.name)
      println(x)
    }
    }
    println("有"+set.size+"女同学参加考试")
  }

  def work3(studentRDD: RDD[Student]): Unit = {
    val students = studentRDD.filter(_.classNumb == "12").collect()
    val set = mutable.Set[String]()

    students.foreach { x => {
      set.add(x.name)

    }
    }
    set.foreach(println)
    println("12班有" + set.size + " 同学参加考试")
  }

  def work4(studentRDD: RDD[Student]): Unit = {
    val students = studentRDD.filter(_.course == "chinese").collect()
    var sum = 0L
    var count =0L
    for(i<-students) {
      println(i)
      sum+=i.score
      count+=1
    }

    println("语文平均分为" + sum/count )
  }

  def work5(studentRDD: RDD[Student]): Unit = {
    val students = studentRDD.map(x=>(x.name,(x.score,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,x._2._1/x._2._2)).collect()

    students.foreach(println)

  }

  def work6(studentRDD: RDD[Student]): Unit = {
    val students = studentRDD.filter(_.classNumb == "12").map(x => (x.score, 1)).reduce((x,y) => (x._1+y._1, x._2+ y._2))

    println("12班的平均分为"+ (students._1/students._2))

  }

  def work7(studentRDD: RDD[Student]): Unit = {
    val students = studentRDD.filter(x =>x.classNumb == "13" && x.sex == "男" ).map(x => (x.score, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    println("13班的男生平均分为" + (students._1 / students._2))

  }

  def work8(studentRDD: RDD[Student]): Unit = {
    val students = studentRDD.filter(x => x.classNumb == "13" && x.sex == "女").map(x => (x.score, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println("13班的女生平均分为" + (students._1 / students._2))
  }

  def work9(studentRDD: RDD[Student]): Unit = {
    val scores = studentRDD.filter(x => x.course == "chinese").map(x => x.score).collect()
    scores.foreach(println)

    println("语文最高分为" +     scores .last)
  }

  def work10(studentRDD: RDD[Student]): Unit = {
    val scores = studentRDD.filter(x => x.course == "chinese" && x.classNumb== "12" ).map(x => x.score).collect()
    scores.foreach(println)

    println("12班语文最低分为" + scores.take(1).mkString)
  }

  def work11(studentRDD: RDD[Student]): Unit = {
    val scores = studentRDD.filter(x => x.sex == "女" && x.classNumb == "12").map(x =>(x.name, x.score)).reduceByKey(_+_).filter(x=>x._2 >150).collect()
    scores.foreach(println)

    println("总成绩大于150分的12班女生有" + scores.length+"个")
  }

  def work12(studentRDD: RDD[Student]): Unit = {

    val scores = studentRDD.filter(x => {
      var flag = true
      if(x.course =="math"){
        if(x.score<70){
          flag = false
        }
      }
      x.age >= 19 && flag
    } ).map(x => (x.name, x.score)).reduceByKey(_ + _).filter(x => x._2 > 150).collect()
    scores.foreach(println)

    println("总成绩大于150分且数学大于等于70，年龄大于等于19岁的学生的平均成绩为" + scores.mkString)
  }
  case  class Student(var classNumb :String, var name:String, age:Long, sex:String, course:String, score:Long ){
    override def toString: String = {
      classNumb +","+name+","+age+","+sex+","+course+","+score
    }
  }




}

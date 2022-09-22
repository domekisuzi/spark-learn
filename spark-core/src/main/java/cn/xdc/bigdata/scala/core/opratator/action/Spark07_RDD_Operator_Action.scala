package cn.xdc.bigdata.scala.core.opratator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val user = new User()


    // RDD 算子中传递的函数是会包含闭包操作，拿，额就会进行检测功能
    // 闭包检测
    rdd.foreach(num => {
      println("age = " + (user.age +  num))
    })
    sc.stop()
  }
//
//  class User extends Serializable {
//    var age = 30
//  }

//样例类在编译时，会自动混入序列化特质
  case class User(){
    var age = 30
  }
}

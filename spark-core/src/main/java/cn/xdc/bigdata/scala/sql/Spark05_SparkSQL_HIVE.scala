package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 弱类型 查找
 */
object Spark05_SparkSQL_HIVE {
  def main(args: Array[String]): Unit = {
//    System.setProperties( )
    //创建环境
//    指定这个后面的环境貌似很重要，否则会爆很奇怪的错误

    // 需要启动元数据服务，否则从 windows（其他地方）这里无法访问，或者hiveserver2(x) ?!总之需要给其他地方访问hive的方法
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .config("hive.exec.scratchdir", "hdfs://hadoop102:8020/tmp/hive")
      .config(sparkConf).enableHiveSupport().getOrCreate()
    //TODO  执行逻辑操作
    val sc = spark.sparkContext

    spark.sql("show tables").show
    spark.close()
  }

}

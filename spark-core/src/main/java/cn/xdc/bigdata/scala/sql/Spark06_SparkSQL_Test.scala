package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 弱类型 查找
 */
object Spark06_SparkSQL_Test {
  def main(args: Array[String]): Unit = {
//    System.setProperties( )
    //创建环境
//    指定这个后面的环境貌似很重要，否则会爆很奇怪的错误

    // 需要启动元数据服务，否则从 windows（其他地方）这里无法访问，或者hiveserver2(x) ?!总之需要给其他地方访问hive的方法
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .config("hive.exec.scratchdir", "hdfs://hadoop102:8020/tmp/hive")
      .config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sql(
      """ CREATE TABLE `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    ).show
    spark.sql("load data local inpath 'datas/user_visit_action.txt' into table  user_visit_action")

    spark.sql("""CREATE TABLE `product_info`(
                | `product_id` bigint,
                | `product_name` string,
                | `extend_info` string)
                |row format delimited fields terminated by '\t' """.stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/product_info.txt' into table product_info""".stripMargin)
    spark.sql("""CREATE TABLE `city_info`(
                | `city_id` bigint,
                | `city_name` string,
                | `area` string)
                |row format delimited fields terminated by '\t'""".stripMargin)
    spark.sql("""load data local inpath 'input/city_info.txt' into table city_info""")
    spark.sql(""" select * from city_info""")
    spark.close()
  }

}
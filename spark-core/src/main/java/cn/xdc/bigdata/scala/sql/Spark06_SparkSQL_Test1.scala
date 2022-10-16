package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 弱类型 查找
 */
object Spark06_SparkSQL_Test1 {
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
      """
        |select
        |    *
        |from (
        |    select
        |        *,
        |        rank() over( partition by area order by clickCnt desc ) as rank
        |    from (
        |        select
        |           area,
        |           product_name,
        |           count(*) as clickCnt
        |        from (
        |            select
        |               a.*,
        |               p.product_name,
        |               c.area,
        |               c.city_name
        |            from user_visit_action a
        |            join product_info p on a.click_product_id = p.product_id
        |            join city_info c on a.city_id = c.city_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3 where rank <= 3
       """.stripMargin).show
    spark.close()
  }

}
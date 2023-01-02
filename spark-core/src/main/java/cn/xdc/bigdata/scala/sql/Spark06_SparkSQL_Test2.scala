package cn.xdc.bigdata.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * TODO("学完hive,加强数据库后再回头来看这个东西")
 */
object Spark06_SparkSQL_Test2 {
  def main(args: Array[String]): Unit = {
//    System.setProperties( )
    //创建环境
//    指定这个后面的环境貌似很重要，否则会爆很奇怪的错误

    // 需要启动元数据服务，否则从 windows（其他地方）这里无法访问，或者hiveserver2(x) ?!总之需要给其他地方访问hive的方法
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val spark =  SparkSession.builder().config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .config("hive.exec.scratchdir", "hdfs://hadoop102:8020/tmp/hive")
      .config(sparkConf).enableHiveSupport().getOrCreate()
    // 查询基本数据
    // 补 hive 窗口查询
    spark.sql(
      """
        |  select
        |     a.*,
        |     p.product_name,
        |     c.area,
        |     c.city_name
        |  from user_visit_action a
        |  join product_info p on a.click_product_id = p.product_id
        |  join city_info c on a.city_id = c.city_id
        |  where a.click_product_id > -1
             """.stripMargin).createOrReplaceTempView("t1")

    // 根据区域，商品进行数据聚合
//    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))
//    spark.sql(
//      """
//        |  select
//        |     area,
//        |     product_name,
//        |     count(*) as clickCnt,
//        |     cityRemark(city_name) as city_remark
//        |  from t1 group by area, product_name
//             """.stripMargin).createOrReplaceTempView("t2")
//
//    // 区域内对点击数量进行排行
//    spark.sql(
//      """
//        |  select
//        |      *,
//        |      rank() over( partition by area order by clickCnt desc ) as rank
//        |  from t2
//             """.stripMargin).createOrReplaceTempView("t3")
//
//    // 取前3名
//    spark.sql(
//      """
//        | select
//        |     *
//        | from t3 where rank <= 3
//             """.stripMargin).show(false)

    spark.close()
  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  // 自定义聚合函数：实现城市备注功能
  // 1. 继承Aggregator, 定义泛型
  //    IN ： 城市名称
  //    BUF : Buffer =>【总点击数量，Map[（city, cnt）, (city, cnt)]】
  //    OUT : 备注信息
  // 2. 重写方法（6）
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    // 更新缓冲区数据
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCount = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, newCount)
      buff
    }

    // 合并缓冲区数据
    override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
      buff1.total += buff2.total

      val map1 = buff1.cityMap
      val map2 = buff2.cityMap

      // 两个Map的合并操作
      //            buff1.cityMap = map1.foldLeft(map2) {
      //                case ( map, (city, cnt) ) => {
      //                    val newCount = map.getOrElse(city, 0L) + cnt
      //                    map.update(city, newCount)
      //                    map
      //                }
      //            }
      map2.foreach {
        case (city, cnt) => {
          val newCount = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCount)
        }
      }
      buff1.cityMap = map1
      buff1
    }


    // 将统计的结果生成字符串信息
    override def finish(buff: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalcnt = buff.total
      val cityMap = buff.cityMap

      // 降序排列
      val cityCntList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      val hasMore = cityMap.size > 2
      var rsum = 0L
      cityCntList.foreach {
        case (city, cnt) => {
          val r = cnt * 100 / totalcnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
        }
      }
      if (hasMore) {
        remarkList.append(s"其他 ${100 - rsum}%")
      }

      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }


}

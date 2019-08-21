package com.al.scala.exam.week4

/*
* 孟祥琰
 */
import java.util.Properties

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object AreaTop3Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // 指定获取数据的开始时间和结束时间
    val startDate = "2019-08-15"
    val endDate = "2019-08-15"

    spark.sql("CREATE TABLE IF NOT EXISTS user_visit_action (session_date string, user_id int, session_id string, page_id int, action_time string, search_keyword string, click_category_id int, click_product_id int, order_category_ids string, order_product_ids string, pay_category_ids string, pay_product_ids string, city_id int)")
    spark.sql("LOAD DATA LOCAL INPATH  'dir/user_visit_action.txt' INTO TABLE user_visit_action")
    // 创建表product_info并加载数据
    spark.sql("CREATE TABLE IF NOT EXISTS product_info (product_id int, product_name string, extend_info string)")
    spark.sql("LOAD DATA LOCAL INPATH  'dir/product_info.txt' INTO TABLE product_info")

    //从表user_visit_action中获取基础数据：用户点击流日志
    spark.sql(
      "select "
        + "city_id,"
        + "click_product_id"
        + "from user_visit_action "
        + "where click_product_id is not null "
        + "and session_date>='" + startDate + "' " +
        "and session_date<='" + endDate + "'")
      .createOrReplaceTempView("user_visit")

    // 读取mysql 上的city_info 表
    spark.read.jdbc(getProperties()._2, "city_info", getProperties()._1)
      .createOrReplaceTempView("city_info")

    // 将点击流日志和城市信息进行join，生成临时表tmp_click_product_basic
    spark.sql("select " +
      "c.city_id cityId, c.city_name cityName, c.area area, u.click_product_id click_product_id " +
      "from city_info c join user_visit u on c.city_id = u.city_id")
      .createOrReplaceTempView("tmp_click_product_basic")

    // 注册udf 函数
    spark.udf.register("city_concat", new GroupConcatDistinctUDAF)

    // 根据表tmp_click_product_basic，统计各区域商品点击次数并生成临时表tmp_area_product_click_count
    spark.sql("select area, click_product_id product_id, " +
      "count(1) click_count, city_concat(concat(cityId, ':', cityName)) city_Infos " +
      "from tmp_click_product_basic group by area, click_product_id")
      .createOrReplaceTempView("tmp_area_product_click_count")

    // 注册 product_info 临时表
    spark.sql("select product_id, product_name , extend_info from product_info ").createOrReplaceTempView("product_info")

    // 注册 udf 函数
    spark.udf.register("getjson", new GetJsonObjectUDF, StringType)

    // 生成临时表tmp_area_fullprod_click_count
    spark.sql("select area, a.product_id product_id, click_count, city_infos, product_name, " +
      "(case getjson(extend_info, 'product_status') when '1' then '第三方' when '0' then '自营' end) product_status " +
      "from tmp_area_product_click_count a join product_info p on a.product_id = p.product_id")
      .createOrReplaceTempView("tmp_area_fullprod_click_count")


    // 将tmp_area_fullprod_click_count进行统计每个区域的top3热门商品
    val res: DataFrame = spark.sql("select area, " +
      "(case when area='华北' or area='华东' then 'A级' when area='华南' or area='华中' then 'B级' when area='西南' or area='西北' then 'C级' when area='东北' then 'D级' end) area_level, " +
      "product_id, city_infos, click_count, product_name, product_status from " +
      "(select area, product_id, city_infos, click_count, product_name, product_status, " +
      "row_number() over(partition by area sort by click_count desc) rank " +
      "from tmp_area_fullprod_click_count) t " +
      "where rank <= 3")


    // 将结果导出到mysql
    res.write.jdbc(getProperties()._2, "area_top3_product", getProperties()._1)


    spark.stop()
  }


  /**
    * 请求数据库的配置信息
    *
    * @return
    */
  def getProperties() = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://hadoop02:3306/exam?useUnicode=true&characterEncoding=utf8"
    (prop, url)
  }

}

case class user_visit(click_product_id: String, city_id: String)

case class product(product_id: Int, product_name: String, extend_info: String)

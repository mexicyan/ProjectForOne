package com.al.scala.exam

import java.util
import java.util.Properties

import com.al.scala.exam.week4.{GetJsonObjectUDF, GroupConcatDistinctUDAF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：按区域统计top3热门商品
  * 实现思路：
  *   1. 创建user_visit_action、product_info表到当前SparkSession并加载数据
  *   2. 从表user_visit_action中获取基础数据：用户点击流日志
  *     获取的字段有：city_id、click_product_id
  *     获取条件：日期范围为：startDate="2019-08-15"，endDate="2019-08-15"
  *              click_product_id（用户点击商品id字段）限定为不为空
  *   3. 获取mysql的city_info表的城市信息
  *   4. 将点击流日志和城市信息进行join，生成临时表tmp_click_product_basic，字段有：cityId, cityName, area, click_product_id
  *   5. 根据表tmp_click_product_basic，统计各区域商品点击次数并生成临时表tmp_area_product_click_count，字段有：area,product_id,click_count,city_infos
  *     city_infos的统计要求：
  *       因为每个区域有很多城市，需要将各个区域涉及到的城市信息拼接起来，比如华南区有广州和三亚，拼接后的city_infos为："4:三亚,3:广州"，其中数字4和3为city_id
  *       此时需要用到GroupConcatDistinctUDAF函数
  *   6. 将各区域商品点击次数临时表tmp_area_product_click_count的product_id字段去关联商品信息表(product_info)的product_id
  *     product_info表的extend_info字段值为json串，需要特殊处理："0"和"1"分别代表了自营和第三方商品
  *     需要用GetJsonObjectUDF函数是从json串中获取指定字段的值，如果product_status为0，返回值为"自营"，如果为1，返回值为"第三方"
  *     生成临时表tmp_area_fullprod_click_count的字段有：
  *     area,product_id,click_count,city_infos,product_name,product_status
  *   7. 将tmp_area_fullprod_click_count进行统计每个区域的top3热门商品（使用开窗函数进行子查询）
  *      统计过程中的外层查询需要增加area_level字段，即按照区域进行分级：
  *      区域有：华北、华东、华南、华中、西北、西南、东北
  *         A级：华北、华东
  *         B级：华南、华中
  *         C级：西北、西南
  *         D级：东北
  *      得到的结果字段有：area、area_level、product_id、city_infos、click_count、product_name、product_status
  *   8. 将结果保存到mysql的area_top3_product表中
  *
  */
object AreaTop3Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 指定获取数据的开始时间和结束时间
    val startDate = "2019-08-15"
    val endDate = "2019-08-15"

    // 创建表user_visit_action并加载数据
//    spark.sql("CREATE TABLE IF NOT EXISTS user_visit_action (session_date string, user_id int, session_id string, page_id int, action_time string, search_keyword string, click_category_id int, click_product_id int, order_category_ids string, order_product_ids string, pay_category_ids string, pay_product_ids string, city_id int)")
//    spark.sql("LOAD DATA LOCAL INPATH  'dir/user_visit_action.txt' INTO TABLE user_visit_action")
//    // 创建表product_info并加载数据
//    spark.sql("CREATE TABLE IF NOT EXISTS product_info (product_id int, product_name string, extend_info string)")
//    spark.sql("LOAD DATA LOCAL INPATH  'dir/product_info.txt' INTO TABLE product_info")

    // 注册自定义函数
    spark.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)
    spark.udf.register("get_json_object", new GetJsonObjectUDF, DataTypes.StringType)

    // 查询用户指定日期范围内的点击行为数据：<city_id, 点击行为>
    // <city_id,click_product_id>
    val cityId2ClickActionRDD = getCityId2ClickActionRDD(spark, startDate, endDate)

    // 从MySQL表（city_info）中查询城市信息，返回的格式为：<cityId, cityInfo>
    val cityId2CityInfoRDD = getCityId2CityInfoRDD(spark)

    // 生成点击商品基础信息临时表
    // 字段：cityId, cityName, area, productId
    generateTempClickProductBasicTable(spark, cityId2ClickActionRDD, cityId2CityInfoRDD)

    // 生成各区域商品点击次数
    // 字段：area,product_id,click_count,city_info
    generateTempAreaProductClickCountTable(spark)

    // 生成包含完整商品信息的各区域各商品点击次数的临时表
    generateTempAreaFullProductClickCountTable(spark)

    // 使用开窗函数获取各个区域点击次数top3热门商品
    val areaTop3ProductDF = getAreaTop3ProductRDD(spark)

    areaTop3ProductDF.show()

    // 存储
    persistAreaTop3Product(areaTop3ProductDF)

    spark.stop()
  }

  /**
    * 查询指定日期范围内的点击行为数据
    *
    * @param spark
    * @param startDate
    * @param endDate
    * @return
    */
  private def getCityId2ClickActionRDD(spark: SparkSession, startDate: String, endDate: String) = { // 从user_visit_action基础表中查询用户访问行为数据

    // 第一个限定：click_product_id限定为不为空的访问行为，这个字段的值就代表点击行为
    // 第二个限定：在使用者指定的日期范围内的数据
    val sql =
    "select " +
      "city_id, " +
      "click_product_id product_id " +
      "from user_visit_action " +
      "where click_product_id is not null " +
      "and session_date>='" + startDate + "'" +
      "and session_date<='" + endDate + "'"

    val clickActionDF = spark.sql(sql)

    // 把生成的DataFrame转换为RDD
    val clickActionRDD = clickActionDF.rdd

    val cityId2ClickActionRDD = clickActionRDD.map(row => (row.getInt(0), row))

    cityId2ClickActionRDD
  }

  /**
    * 获取城市信息
    *
    * @param spark
    * @return
    */
  private def getCityId2CityInfoRDD(spark: SparkSession) = {
    val propsAndUrl = getProperties()
    val props = propsAndUrl._1
    val url = propsAndUrl._2
    val tablename = "city_info"

    val df: DataFrame = spark.read.jdbc(url, tablename, props)

    val cityInfoRDD = df.rdd
    val cityId2CityInfoRDD = cityInfoRDD.map(row => (String.valueOf(row.get(0)).toInt, row))

    cityId2CityInfoRDD
  }

  /**
    * 生成点击商品基础信息临时表
    *
    * @param spark
    * @param cityId2ClickActionRDD
    * @param cityId2CityInfoRDD
    */
  private def generateTempClickProductBasicTable(spark: SparkSession, cityId2ClickActionRDD: RDD[(Int, Row)], cityId2CityInfoRDD: RDD[(Int, Row)]) = { // 将点击行为和城市信息进行关联：join

    val joinedRDD = cityId2ClickActionRDD.join(cityId2CityInfoRDD)

    // 将上面的join后的结果数据转换成一个RDD<Row>,
    // 是因为转换成Row后才能将RDD转换为DataFrame
    val mappedRDD = joinedRDD.map(tup => {
      val cityId = tup._1
      val clickAction = tup._2._1
      val cityInfo = tup._2._2
      val productId = clickAction.getInt(1)
      val cityName = cityInfo.getString(1)
      val area = cityInfo.getString(2)

      Row(cityId, cityName, area, productId)
    })

    // 构建schema信息
    val structFields = new util.ArrayList[StructField]
    structFields.add(DataTypes.createStructField("city_id", DataTypes.IntegerType, true))
    structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("product_id", DataTypes.IntegerType, true))

    val schema = DataTypes.createStructType(structFields)

    // 生成DataFrame
    val df = spark.createDataFrame(mappedRDD, schema)

    // 注册为临时表，字段：cityId, cityName, area, productId
    df.createTempView("tmp_click_product_basic")
  }

  /**
    * 生成各区域商品点击次数
    *
    * @param spark
    */
  private def generateTempAreaProductClickCountTable(spark: SparkSession) = {

    // 计算出各区域商品的点击次数
    // 可以获取到每个area下的每个product_id的城市信息，并拼接为字符串
    val sql =
    "select " +
      "area," +
      "product_id," +
      "count(*) click_count," +
      "group_concat_distinct(concat_ws(':',city_id,city_name)) city_infos " +
      "from tmp_click_product_basic " +
      "group by area,product_id"

    val df = spark.sql(sql)

    // area,product_id,click_count,city_info
    df.createTempView("tmp_area_product_click_count")
  }

  /**
    * 生成包含完整商品信息的各区域各商品点击次数的临时表
    *
    * @param spark
    */
  private def generateTempAreaFullProductClickCountTable(spark: SparkSession) = {
    /**
      * 将之前得到的各区域商品点击次数表(tmp_area_product_click_count)的product_id字段
      * 去关联商品信息表(product_info)的product_id
      * 其中product_status需要特殊处理：0,1分别代表了自营和第三方商品，放在了一个json里
      * 实现GetJsonObjectUDF()函数是从json串中获取指定字段的值
      * if()函数进行判断，如果product_status为0，就是自营商品，如果为1，就是第三方商品
      * 此时该表的字段有：
      * area,product_id,click_count,city_infos,product_name,product_status
      */
    val sql =
      "select " +
        "tapcc.area," +
        "tapcc.product_id," +
        "tapcc.click_count," +
        "tapcc.city_infos," +
        "pi.product_name," +
        "if(get_json_object(pi.extend_info,'product_status')='0'," +
        "'自营','第三方') product_status " +
        "from tmp_area_product_click_count tapcc " +
        "join product_info pi " +
        "on tapcc.product_id=pi.product_id"

    val df = spark.sql(sql)

    df.createTempView("tmp_area_fullprod_click_count")
  }

  /**
    * 获取区域top3热门商品
    *
    * @param sparkSesion
    * @return
    */
  private def getAreaTop3ProductRDD(sparkSesion: SparkSession) = {
    /**
      * 使用开窗函数进行子查询
      * 按照area进行分组，给每个分组内的数据按照点击次数进行降序排序，并打一个行标
      * 然后在外层查询中，过滤出各个组内行标排名前3的数据
      * 按照区域进行分级：
      * 华北、华东、华南、华中、西北、西南、东北
      * A级：华北、华东
      * B级：华南、华中
      * C级：西北、西南
      * D级：东北
      */
    val sql =
      "select " +
        "area," +
        "case " +
        "when area='华北' or area='华东' then 'A级' " +
        "when area='华南' or area='华中' then 'B级' " +
        "when area='西北' or area='西南' then 'C级' " +
        "else 'D级' " +
        "end area_level," +
        "product_id," +
        "click_count," +
        "city_infos," +
        "product_name," +
        "product_status " +
        "from(" +
        "select " +
        "area," +
        "product_id," +
        "click_count," +
        "city_infos," +
        "product_name," +
        "product_status," +
        "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
        "from tmp_area_fullprod_click_count " +
        ") t " +
        "where rank <= 3"

    sparkSesion.sql(sql)
  }

  /**
    * 持久化数据到mysql
    *
    * @param areaTop3ProductDF
    */
  def persistAreaTop3Product(areaTop3ProductDF: DataFrame) = {
    val propsAndUrl = getProperties()
    val props = propsAndUrl._1
    val url = propsAndUrl._2
    val tablename = "area_top3_product"
    areaTop3ProductDF.write.mode(SaveMode.Append).jdbc(url, tablename, props)
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

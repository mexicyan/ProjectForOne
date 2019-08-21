package com.ETL

import java.util.Properties

import com.utils.Utils2Type
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ProvinceCityCount {
  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()



    val lines = spark.read.textFile("dir/originRes/*").rdd

    val RddAll = getAll(lines,",")

    val provinceAndCity: RDD[Row] = RddAll.map(x => {
      val province: String = x.getString(24)
      val city: String = x.getString(25)
      Row(province, city)
    })



    val structtypeProvinceAndCity = StructType(
      Seq(
        StructField("province",StringType),
        StructField("city",StringType)
      )
    )

    val df: DataFrame = spark.createDataFrame(provinceAndCity, structtypeProvinceAndCity)


    df.createOrReplaceTempView("ProvinceAndCity")


    val resOfProvAndCity: DataFrame = spark.sql("select count(1) ct,province,city from ProvinceAndCity group by province,city ")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

//    resOfProvAndCity.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop02:3306/exam?useUnicode=true&characterEncoding=utf8","ProvinceAndCityCount",prop)

    resOfProvAndCity.write.partitionBy("province","city").json("dir/out")

    spark.stop()




}
def getAll(rdd: RDD[String],s:String): RDD[Row] ={
  val res: RDD[Row] = rdd.map(t => t.split(",", t.length)).filter(_.length >= 85)
  .map(arr => {
  Row(
  arr(0),
  Utils2Type.toInt(arr(1)),
  Utils2Type.toInt(arr(2)),
  Utils2Type.toInt(arr(3)),
  Utils2Type.toInt(arr(4)),
  arr(5),
  arr(6),
  Utils2Type.toInt(arr(7)),
  Utils2Type.toInt(arr(8)),
  Utils2Type.toDouble(arr(9)),
  Utils2Type.toDouble(arr(10)),
  arr(11),
  arr(12),
  arr(13),
  arr(14),
  arr(15),
  arr(16),
  Utils2Type.toInt(arr(17)),
  arr(18),
  arr(19),
  Utils2Type.toInt(arr(20)),
  Utils2Type.toInt(arr(21)),
  arr(22),
  arr(23),
  arr(24),
  arr(25),
  Utils2Type.toInt(arr(26)),
  arr(27),
  Utils2Type.toInt(arr(28)),
  arr(29),
  Utils2Type.toInt(arr(30)),
  Utils2Type.toInt(arr(31)),
  Utils2Type.toInt(arr(32)),
  arr(33),
  Utils2Type.toInt(arr(34)),
  Utils2Type.toInt(arr(35)),
  Utils2Type.toInt(arr(36)),
  arr(37),
  Utils2Type.toInt(arr(38)),
  Utils2Type.toInt(arr(39)),
  Utils2Type.toDouble(arr(40)),
  Utils2Type.toDouble(arr(41)),
  Utils2Type.toInt(arr(42)),
  arr(43),
  Utils2Type.toDouble(arr(44)),
  Utils2Type.toDouble(arr(45)),
  arr(46),
  arr(47),
  arr(48),
  arr(49),
  arr(50),
  arr(51),
  arr(52),
  arr(53),
  arr(54),
  arr(55),
  arr(56),
  Utils2Type.toInt(arr(57)),
  Utils2Type.toDouble(arr(58)),
  Utils2Type.toInt(arr(59)),
  Utils2Type.toInt(arr(60)),
  arr(61),
  arr(62),
  arr(63),
  arr(64),
  arr(65),
  arr(66),
  arr(67),
  arr(68),
  arr(69),
  arr(70),
  arr(71),
  arr(72),
  Utils2Type.toInt(arr(73)),
  Utils2Type.toDouble(arr(74)),
  Utils2Type.toDouble(arr(75)),
  Utils2Type.toDouble(arr(76)),
  Utils2Type.toDouble(arr(77)),
  Utils2Type.toDouble(arr(78)),
  arr(79),
  arr(80),
  arr(81),
  arr(82),
  arr(83),
  Utils2Type.toInt(arr(84))
  )
})
  res
}

}
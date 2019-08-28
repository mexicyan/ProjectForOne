package com.exam.week1

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object businessarea {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

//    val file = spark.sparkContext.textFile("dir/json")

//    val file = spark.sparkContext.textFile("dir/json")
    val file = spark.read.textFile("dir/json").rdd
    file.foreach(println)


    val result1: RDD[String] = file.map(line => {
      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      val jsonparse = JSON.parseObject(line)
      // 判断状态是否成功
      val status = jsonparse.getIntValue("status")
      if (status == 1) {
        // 接下来解析内部json串，判断每个key的value都不能为空
        val regeocodeJson = jsonparse.getJSONObject("regeocode")
        if (regeocodeJson != null && !regeocodeJson.keySet().isEmpty) {
          //获取pois数组
          val poisArray = regeocodeJson.getJSONArray("pois")
          if (poisArray != null && !poisArray.isEmpty) {
            // 循环输出
            for (item <- poisArray.toArray) {
              if (item.isInstanceOf[JSONObject]) {
                val json = item.asInstanceOf[JSONObject]
                buffer.append(json.getString("businessarea"))
              }
            }
          }
        }
      }
      buffer.mkString(",")
    })



//    result1.flatMap(x => {
//      x.split(",").map((_,1))
//    }).reduceByKey(_+_)
//      .foreach(println)

    result1.flatMap(x => {
      x.split(",")
    }).map(x => (x,1))
      .reduceByKey(_+_)
      .foreach(println)



    //    result1.foreach(println)

    //      val linearr: Array[String] = x.split(",")
    //      (linearr.toList(0),linearr.size)
    //    }).foreach(println)

  }

}

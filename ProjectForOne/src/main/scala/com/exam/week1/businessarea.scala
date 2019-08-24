package com.exam.week1

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object businessarea {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("dir/json/json.txt")
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

    //    result1.foreach(println)

    result1.flatMap(line=>{
      line.split(",").map((_,1))
    }).filter(x=> !x._1.equals("[]")).reduceByKey(_+_).foreach(println)


    //    result1.flatMap(x=>{
    //      val linearr: Array[String] = x.split(",")
    //      (linearr.toList(0),linearr.size)
    //    }).foreach(println)

  }

}

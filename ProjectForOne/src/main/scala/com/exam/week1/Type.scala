package com.exam.week1

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Type {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("dir/json/json.txt")
    val result2: RDD[String] = file.map(line => {
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
                val strarr: Array[String] = json.getString("type").split(";")
                strarr.foreach(x => buffer.append(x))
                //                buffer.append(json.getString("type"))
              }
            }
          }
        }
      }
      buffer.mkString(",")
    })
    //    result2.foreach(println)
    val temp: RDD[(String, Int)] = result2.flatMap(line => {
      line.split(",").map((_, 1))
    })
    //    temp.foreach(println)

    temp.reduceByKey(_+_).foreach(println)
  }

}

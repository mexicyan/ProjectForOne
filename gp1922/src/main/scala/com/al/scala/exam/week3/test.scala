package com.al.scala.exam.week3

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 获取数据生成DataFrame
    val df = spark.read.json("dir/JsonTest02.json")

    // 注册一张临时表, 使用最多. 该表的作用域在当前的Session中
    df.createOrReplaceTempView("t_json")
    //    1. 统计每个用户充值总金额并降序排序（10分）
    //    spark.sql("select phoneNum username,sum(money) moneys from t_json where status='1' group by phoneNum order by moneys desc").show()
    //    2. 统计所有系统类型登录总次数并降序排序（10分）
    //    spark.sql("select terminal ,count(terminal) count from t_json group by terminal order by count desc").show()
    //    3. 统计各省的每个用户登录次数的Top3（20分）
    spark.sql("select * from (select province ,count(province) count,rank() over(order by count(province) desc) rank from t_json group by province order by count desc) t where t.rank<=3").show()

    println("-------------------------------------下面是SparkCore-----------------------------------------")

    val file = sc.textFile("dir/JsonTest02.json")
    val usernameAndmoney: RDD[(String, Int)] = file.map(myrecord => {
      val json = JSON.parseObject(myrecord)

      val username = json.get("phoneNum").toString
      var moneys = 0
      if (json.get("status").toString.toInt == 1 && json.get("money") != null) moneys = json.get("money").toString.toInt
      (username, moneys)
    }
    )
    val result1: RDD[(String, Int)] = usernameAndmoney.reduceByKey(_ + _).sortBy(_._2, false)
    //    1. 统计每个用户充值总金额并降序排序（10分）
    //    result1.collect.foreach(x=>println("用户：  "+x._1+"   金钱："+x._2))

    val terminalCount: RDD[(String, Int)] = file.map(line => {
      val json = JSON.parseObject(line)
      (json.get("terminal").toString, 1)
    }).reduceByKey(_ + _).sortBy(_._2, false)
    //    2. 统计所有系统类型登录总次数并降序排序（10分）
    //    terminalCount.collect.foreach(x=>println("系统：  "+x._1+"   数量："+x._2))

    val provinceAndCount: RDD[(String, Int)] = file.map(line => {
      val json = JSON.parseObject(line)
      (json.get("province").toString, 1)
    }).reduceByKey(_ + _).sortBy(_._2, false)
    val result3: Array[(String, Int)] = provinceAndCount.collect
    //    3. 统计各省的每个用户登录次数的Top3（20分）
    for (i <- 0 to 2) {
      println("省份：   " + result3(i)._1 + "   数量：   " + result3(i)._2)
    }


  }
}
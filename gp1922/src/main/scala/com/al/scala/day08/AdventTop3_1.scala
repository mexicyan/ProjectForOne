package com.al.scala.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AdventTop3_1 {

  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf = new SparkConf().setAppName("adventtop3").setMaster("local[2]")

    val sc = new SparkContext(conf)

    //获取数据并切分
    val logs = sc.textFile("D://data/Advert.log")

    val logArr: RDD[Array[String]] = logs.map(_.split("\t"))

    //提取要分析需求需要的数据
    val provinceAndAdid: RDD[(String, Int)] = logArr.map(x => (x(1) + "_" + x(4),1))

    //将每个省份对应的的广告进行点击量的统计
    val aggProvinceAndAdid: RDD[(String, Int)] = provinceAndAdid.reduceByKey(_+_)

    val provinceAndadIdTup: RDD[(String, String, Int)] = aggProvinceAndAdid.map(tup => {
      val splited: Array[String] = tup._1.split("_")
      val province = splited(0)
      val adid = splited(1)
      (province, adid, tup._2)
    })
    provinceAndadIdTup

    //按照省份进行分组
    val groupedPro: RDD[(String, Iterable[(String, String, Int)])] = provinceAndadIdTup.groupBy(_._1)

    //组内排序
    val res: RDD[(String, List[(String, String, Int)])] = groupedPro.mapValues(x => x.toList.sortWith(_._3 > _._3).take(3))

    println(res.collect().toBuffer)

    sc.stop()




  }
}

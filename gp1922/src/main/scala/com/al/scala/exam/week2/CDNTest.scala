package com.al.scala.exam.week2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 孟祥琰
 */
object CDNTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CDNTest")
    val sc = new SparkContext(conf)

    val input = sc.textFile("d://data/cdn/cdn.txt").cache()

    // 统计独立IP访问量前10位
//    ipStatics(input)

    //统计每个视频独立IP数
//    videoIpStatics(input)

    // 统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()
  }

  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {

    val filter: RDD[String] = data.filter(x => !x.split(" ")(8).contains("404"))
    val hourAndMuch: RDD[(String, Long)] = filter.map(x => {
      val splits: Array[String] = x.split(" ")
      val hour = splits(3).split(":")(1)
      val much = splits(9).toLong
      (hour, much)
    })

    val grouped: RDD[(String, Iterable[Long])] = hourAndMuch.groupByKey()

    val value: RDD[(String, Long)] = grouped.map(x => (x._1,x._2.sum)).sortByKey()

    val values: RDD[(String, Long)] = value.map(x => (x._1,x._2/1024/1024/1024))

    val res: RDD[(String, Long)] = values.map(x=>(x._1,x._2)).sortByKey()

    println("统计一天中每个小时间的流量:")

    res.collect.foreach(x => println(x._1+"时 CDN流量="+x._2+"G"))




  }

  // 统计每个视频独立IP数
  def videoIpStatics(data: RDD[String]): Unit = {

    val nameAndIp: RDD[(String, String)] = data.filter(x => x.contains(".mp4")).map(x => {
      val splits: Array[String] = x.split(" ")
      val name = splits(6).split("/")(3)

      val ip = splits(0)
      (name, ip)
    })

    val red: RDD[(String, Iterable[(String, String)])] = nameAndIp.groupBy(_._1)



    val reduces: RDD[(String, Array[String])] = red.mapValues(x => x.toArray.map(x => x._2).distinct).cache()

    val res: Array[(String, Int)] = reduces.map(x => (x._1, x._2.size)).sortBy(_._2,false).take(10)



      println("统计每个视频独立IP数:")
      res.foreach(x => println("视频："+x._1+" 独立IP数："+x._2))
  }

  // 统计独立IP访问量前10位
  def ipStatics(data: RDD[String]): Unit = {
    val ips: RDD[(String, Int)] = data.map(line => {
      val splits: Array[String] = line.split(" ")
      val ip = splits(0)
      (ip, 1)
    })

    val reduces: RDD[(String, Int)] = ips.reduceByKey(_+_)


    val res: Array[(String, Int)] = reduces.sortBy(_._2,false).take(10)

    println("统计独立IP访问量前10位:")
    res.foreach(println)
    }
}

package com.al.scala.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SubjectAccessCache {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val subjects = Array("http://java.learn.com","http://h5.learn.com","http://bigdata.learn.com","http://ui.learn.com","http://android.learn.com")


    //获取数据并切分成元祖
    val tups: RDD[(String, Int)] = sc.textFile("D://data/subjectaccess/access.txt")
      .map(line => (line.split("\t")(1), 1))


    //开始聚合，得到各个模块的访问量
    val aggred: RDD[(String, Int)] = tups.reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK)

    //按照学科信息进行分组并组内排序取top3
    for (subject <- subjects){
      //按照学科进行过滤,得到该学科对应的所有的模块的访问量
      val filtered: RDD[(String, Int)] = aggred.filter(_._1.startsWith(subject))
      val res: Array[(String, Int)] = filtered.sortBy(_._2,false).take(3)
      println(res.toBuffer)
    }

    sc.stop()
  }
}

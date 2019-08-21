package com.al.scala.zuoye

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("dir/file.txt")

    val sizes: RDD[Int] = lines.map(_.split(" ").size)

    println(sizes.max())

  }
}

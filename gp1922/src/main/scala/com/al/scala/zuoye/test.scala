package com.al.scala.zuoye

import org.apache.spark.{SparkConf, SparkContext}

object test {

  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3,4,5,6,7,8,9)

    val conf = new SparkConf().setAppName("Test").setMaster("local")
    val sc = new SparkContext(conf)
   a.grouped(2).toList.flatten.foreach(println)

    val rdd10_1 = sc.parallelize(List(("tom",1),("jerry" ,3),("kitty",2)))
    val rdd10_2 = sc.parallelize(List(("jerry" ,2),("tom",2),("dog",10)))
    val rdd10_3 = rdd10_1 join rdd10_2
    println(rdd10_3.collect().toBuffer)

  }
}

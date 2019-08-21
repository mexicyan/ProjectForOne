package com.al.scala.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object T5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val child_parent_rdd = sc.textFile("D://data/T5.txt").filter(x => {
      if (x.contains("child")) false else true
    })
      .map(x => {
        val str = x.replaceAll("\\s+", " ").split(" ");
        (str(0), str(1))
      }).cache()
    child_parent_rdd.collect().foreach(println)
    val parent_child_rdd = child_parent_rdd.map(x => (x._2, x._1))
    println()
    println()
    parent_child_rdd.collect().foreach(println)
    println()
    println()
    val value: RDD[(String, (String, String))] = child_parent_rdd.join(parent_child_rdd)
    value.collect().foreach(println)
    println()
    println()
    val child_grand_rdd = value
    //（父母，（祖父母，孙子））
    val grandchild_grandparent_rdd = child_grand_rdd.map(x => (x._2._2, x._2._1)).repartition(1).foreach(x =>println("孙子们"+x._1+",姥爷们："+x._2))

  }
}

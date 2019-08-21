package com.al.scala.exam.week3

import org.apache.spark.sql.{DataFrame, SparkSession}


object week3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    val lines: DataFrame = spark.read.json("dir/JsonTest02.json")

    lines.createOrReplaceTempView("t1")

    println("t1")
    spark.sql("select phoneNum,sum(money) cost from t1 where status =='1' group by phoneNum order by cost desc").show()
    println("t2")
    spark.sql("select terminal,count(1) cnt from t1 group by terminal order by cnt desc").show()
    println("t3")
    spark.sql("select province, phoneNum,cnt,rank from(" +
      "select province ,phoneNum,cnt,dense_rank() over(partition by province order by cnt desc) rank from(" +
      "select province,phoneNum,count(1) cnt from t1 group by province ,phoneNum"+
      ") tmp1" +
      ") tmp2 where rank <=3").show



  }
}

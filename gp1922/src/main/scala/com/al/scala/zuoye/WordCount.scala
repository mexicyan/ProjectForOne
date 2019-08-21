package com.al.scala.zuoye

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * 有数据文件test.txt
  * 数据内容：
  * hello java hello
  * hello scala scala
  * hello python
  * ....还有很多数据....
  * 用SparkSQL求单词计数
  * 将结果保存到MySQL
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("sparksqlwoordcount")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val ds2: Dataset[String] = spark.read.textFile("dir/file.txt").flatMap(_.split(" "))




    ds2.createTempView("wc_t")

    val df: DataFrame = spark.sql("select value, count(1) from wc_t group by value")

    df.show()

    // 此处可以将df（结果）传入mysql数据库
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    val url = "jdbc:mysql://hadoop02:3306/mydb01"

    df.write.mode(SaveMode.Append).jdbc(url, "wc", prop)

    spark.stop()
  }
}

package com.al.scala.zuoye

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object idnumn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dtDf: RDD[String] = sc.textFile("C:\\Users\\26950\\Desktop\\test\\input\\data\\six.txt")

    // spark core
    val tupRdd: RDD[(String, (String, String, String))] = dtDf.map(line => {
      val fields = line.split("\t")
      val id = fields(0)
      val time = fields(1)
      val url = fields(2)
      (id, (id, time, url))
    })

    val tmp: RDD[(String, List[(String, String, String)])] = tupRdd.groupByKey
      .mapValues(_.toList.sortWith(_._2 < _._2).take(3))
    //    tmp.map(_._2).foreach(println)

    val res: RDD[List[(String, String, String)]] = tmp.map(_._2)
    res.saveAsTextFile("C:\\Users\\26950\\Desktop\\test\\input\\data\\sixOut.txt")



    val tupRddnew: RDD[(String, String, String)] = dtDf.map(line => {
      val fields = line.split("\t")
      val id = fields(0)
      val time = fields(1)
      val url = fields(2)
      (id, time, url)
    })
    import spark.implicits._
    val frame: DataFrame = tupRddnew.toDF("id", "time", "url")
    val df = frame
    df.createOrReplaceTempView("tb6")
    spark.sql("select id, time, url from(" +
      "select id, time, url, dense_rank() over(partition by id order by time) rank from tb6" +
      ") tmp where rank <= 3").show()

    sc.stop
    spark.stop
  }
}


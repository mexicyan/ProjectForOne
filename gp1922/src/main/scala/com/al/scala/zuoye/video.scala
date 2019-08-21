package com.al.scala.zuoye

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object video {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")



    import spark.implicits._ // 拿到SqlContext上下文
    import org.apache.spark.sql.functions._  // 拿到用于sql处理的常用方法

    val df: DataFrame = spark.read.parquet("dir/video")
    val rdd: RDD[Row] = df.rdd

    val valuee: RDD[((String, String, String, String), List[(Int, Int, Int)])] = rdd.map(x => {
      ((dateFormat.format(new Date(x(15).toString.toLong)), x(0).toString, x(9).toString, x(10).toString), (x(5).toString.toInt, x(6).toString.toInt, 1))
    }).groupByKey().map(x => (x._1, x._2.toList))

    val value: RDD[((String, String, String, String), (Int, Int, Int))] = valuee.map(x => (x._1, x._2.aggregate(0, 0, 0)((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3), (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))))
    val inter= value


//    // 写入数据到mysql的函数
//    val data2MySQL = (it: Iterator[(String, Int)]) => {
//      var conn: Connection = null;
//      var ps: PreparedStatement = null;
//
//      val sql = "insert into videoExam(location, counts, access_date) values(?,?,?)"
//
//      val jdbcUrl = "jdbc:mysql://hadoop02:3306/exam?useUnicode=true&characterEncoding=utf8"
//      val user = "root"
//      val password = "123456"
//
//      try {
//        conn = DriverManager.getConnection(jdbcUrl, user, password)
//        it.foreach(tup => {
//          ps = conn.prepareStatement(sql)
//          ps.setString(1, tup._1)
//          ps.setInt(2, tup._2)
//          ps.setDate(3, new Date(System.currentTimeMillis()))
//          ps.executeUpdate()
//        })
//      } catch {
//        case e: Exception => println(e.printStackTrace())
//      } finally {
//        if (ps != null)
//          ps.close()
//        if (conn != null)
//          conn.close()
//      }
//    }

    import spark.implicits._
    inter.map(x => res1(x._1._1, x._1._2, x._1._3, x._1._4, x._2._1, x._2._2, x._2._3)).toDF().write.mode(SaveMode.Append).format("jdbc")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "videoExam")
      .option("url", "jdbc:mysql://hadoop02:3306/exam")
      .save()
    //    inter.foreach(x => println("时间：" + x._1._1 + "  分类:" + x._1._2 + "  出品地区：" + x._1._3 + "  出品时间：" + x._1._4 + "视频播放时长：" + x._2._1 + "  广告播放时长：" + x._2._2 + "  用户UV:" + x._2._3))


    inter.foreach(println)






    val r2: RDD[((String, String, String, String, String), (Int, Int, Int))] = df.rdd.map(x => {
      ((dateFormat.format(new Date(x(15).toString.toLong)), x(0).toString, x(1).toString, x(9).toString, x(10).toString), (x(2).toString.toInt, x(4).toString.toInt, 1))
    })
    val tempres2: RDD[((String, String, String, String, String), (Int, Int, Int))] = r2.reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    })
    //    tempres2.foreach(x => println("时间：" + x._1._1 + "  分类:" + x._1._2 + "  类型：" + x._1._3 + "  出品地区：" + x._1._4 + "  出品时间" + x._1._5 + "  视频播放时长：" + x._2._1 + "  广告播放时长：" + x._2._2 + "  用户UV:" + x._2._3))


    df.createOrReplaceTempView("tt")

    spark.sql("select * from tt").show()





    spark.stop()

  }


}
case class res1(ct: String, `type`: String, producedarea: String, producedtime: String, videotime: Int, adtime: Int, UV: Int)



package com.Tags

import com.utils.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import redis.clients.jedis.Jedis

object tagsContext {
  def main(args: Array[String]): Unit = {
    //创建上下文
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()


    // 读取数据
    val df = spark.read.parquet("dir/parquet/*")

    val dict = spark.read.textFile("dir/idct/*").rdd

    val AppInfo: RDD[(String,String)] =dict.filter(_.split("\t",-1).length >= 5).map(x => {
      val strings: Array[String] = x.split("\t",-1)
      (strings(1),strings(4))
    })

    AppInfo.foreachPartition(rediscoon)

    val broadcastIPAndName: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(AppInfo.collect().toMap)

    val stopword = spark.read.textFile("dir/keywords/*").rdd.map((_,0)).collectAsMap()
    val bcstopword = spark.sparkContext.broadcast(stopword)



    import spark.implicits._
    // 过滤符合Id的数据
    val test = df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 取出用户Id
      val userId = TagUtils.getOneUserId(row)
      // 接下来的通过Row数据，打上所有标签（按照需求）
      val adtagslist: List[(String, Int)] = TagsAd.makeTags(row)

      val appnamelist: List[(String, Int)] = TagsAppName.makeTags(row,broadcastIPAndName.value)

      val adplatformlist: List[(String, Int)] = TagsChannel.makeTags(row)

      val machinetagslist: List[(String, Int)] = TagsMachine.makeTags(row)

      val keywordList = TagsKeyWords.makeTags(row,bcstopword)

      val locationtagslist: List[(String, Int)] = TagsLocation.makeTags(row)

      (userId,adtagslist++appnamelist++adplatformlist++machinetagslist++keywordList++locationtagslist)
    })
    test.rdd.foreach(println)
  }


  //redis存储
  val rediscoon = (it : Iterator[(String,String)]) => {
    val jedis = new Jedis("hadoop02", 6379)
    it.foreach(tup => {
      jedis.hset("AppInfo",tup._2,tup._1)
    })

    jedis.close()
  }
}

package com.Tags

import com.utils.{JedisConnectionPool, TagUtils}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    // 创建上下文
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()


    // 读取数据
    val df = spark.read.parquet("dir/parquet/*")
    // 读取字段文件
    val map = spark.read.textFile("dir/idct").rdd.map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = spark.sparkContext.broadcast(map)

    // 获取停用词库
    val stopword = spark.read.textFile("dir/keywords").rdd.map((_,0)).collectAsMap()
    val bcstopword = spark.sparkContext.broadcast(stopword)
    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId).rdd
      // 接下来所有的标签都在内部实现
        .mapPartitions(row=>{
      val jedis = JedisConnectionPool.getConnection()
      var list = List[(String,List[(String,Int)])]()
      row.map(row=>{
        // 取出用户Id
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        val appList = TagAPP.makeTags(row,jedis)
        val keywordList = TagKeyWord.makeTags(row,bcstopword)
        val dvList = TagDevice.makeTags(row)
        val loactionList = TagLocation.makeTags(row)
        list:+=(userId,adList++appList++keywordList++dvList++loactionList)
      })
      jedis.close()
      list.iterator
        })
      .reduceByKey((list1,list2)=>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList
      ).foreach(println)

  }
}

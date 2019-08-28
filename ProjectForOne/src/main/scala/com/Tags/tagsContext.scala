package com.Tags

import com.utils.{TagUtils, hbase2writrutils}

import org.apache.spark.broadcast.Broadcast


import org.apache.spark.sql.{SparkSession}
import redis.clients.jedis.Jedis
import com.typesafe.config.ConfigFactory

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf

import org.apache.spark.rdd.RDD


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

    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    //    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.quorum"))
    //    configuration.set("hbase.zookeeper.property.clientPort", load.getString("hbase.zookeeper.property.clientPort"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    import spark.implicits._
    // 过滤符合Id的数据
    val test = df
      .filter(TagUtils.OneUserId)
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

      val locational: List[(String, Int)] = TagsLocation.makeTags(row)

      val business: List[(String, Int)] = BusinessTag.makeTags(row)

      (userId,adtagslist++appnamelist++adplatformlist++machinetagslist++keywordList++locational++business)
    })
    test.rdd.reduceByKey((list1,list2)=>
      // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
      (list1:::list2)
        // List(("APP爱奇艺",List()))
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    )
    .map{
      case(userid,userTag)=>{


        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("20190826"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
      .saveAsHadoopDataset(jobconf)

//    .foreach(println)



//    println(test.count())
  }


  //redis存储
  val rediscoon = (it : Iterator[(String,String)]) => {
    val jedis = new Jedis("192.168.146.11", 6379)
    it.foreach(tup => {
      jedis.hset("AppInfo",tup._2,tup._1)
    })

    jedis.close()
  }
}

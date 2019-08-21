package com.al.scala.day14

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerDemo {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    // 指定响应方式
    prop.put("acks","0")
    // 请求失败重试次数
    prop.put("retries","3")
    // 指定key的序列化方式,key适用于存放数据对应的offset
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式，
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    // 得到生产者的实例
    val producer = new KafkaProducer[String,String](prop)

    // 模拟一些数据并发送给kafka
    for(i <- 1 to 10000){
      val msg = s"${i}:this is kafka data"
      producer.send(new ProducerRecord[String,String]("test",msg))
      Thread.sleep(200)
    }

producer.close()
  }
}
package com.al.scala.day14


import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val prop = new Properties()
    prop.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    // 指定消费者组
    prop.put("group.id","group01")
    // 指定消费位置：earlist/latest/none
    prop.put("auto.offset.reset","earliest")
    // 指定消费的key的反序列方式
    prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列方式
    prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    // 得到Consumer实例
    val consumer = new KafkaConsumer[String,String](prop)


    // 首先需要订阅topic
    consumer.subscribe(Collections.singletonList("test"))
    // 开始消费数据
    while (true){
      val msgs: ConsumerRecords[String, String] = consumer.poll(1000)
      val it = msgs.iterator()
      while (it.hasNext){
        val msg = it.next()
        println(s"partition: ${msg.partition()},offset: ${msg.offset()},key: ${msg.key()},value: ${msg.value()}")
      }
    }
  }
}

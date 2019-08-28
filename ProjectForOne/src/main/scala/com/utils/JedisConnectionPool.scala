package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Jedis连接
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()

  // 设置最大连接数
  config.setMaxTotal(20)
  // 最大空闲
  config.setMaxIdle(10)
  // 创建连接
  val pool = new JedisPool(config,"192.168.146.11", 6379)

  def getConnection():Jedis ={
    pool.getResource
  }
}

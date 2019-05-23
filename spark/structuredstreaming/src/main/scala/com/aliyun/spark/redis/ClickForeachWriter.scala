package com.aliyun.spark.redis

import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.{Jedis, JedisShardInfo}


class ClickForeachWriter(redisHost: String, redisPort: String, redisPassword: String) extends ForeachWriter[Row] {

  var jedis: Jedis = _

  def connect() = {
    val shardInfo: JedisShardInfo = new JedisShardInfo(redisHost, redisPort.toInt)
    shardInfo.setPassword(redisPassword)
    jedis = new Jedis(shardInfo)
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(value: Row): Unit = {

    val asset = value.getString(0)
    val count = value.getLong(1)
    if (jedis == null) {
      connect()
    }

    jedis.hset("click:" + asset, "asset", asset)
    jedis.hset("click:" + asset, "count", count.toString)
    jedis.expire("click:" + asset, 300)

  }

  override def close(errorOrNull: Throwable): Unit = {}
}

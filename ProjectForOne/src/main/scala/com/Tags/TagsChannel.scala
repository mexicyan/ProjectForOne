package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsChannel extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取渠道编号
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
    adplatformproviderid match {
      case v if v >= 100000  => list:+=("CN"+v,1)
    }

    list
  }
}

package com.al.scala.day01

class Test {

  def main(args: Array[String]): Unit = {

    //    fun(16, 8)

    //    fun2(8, 6, 10)

    //    println(fun3(400))

    //    fun4("23:59:59")
    //    fun4("06:06:06")
    //    fun4("06:30:39")
    /*
    当前时间为：23:59:59
    下一秒时间为：00:00:00
    当前时间为：06:06:06
    下一秒时间为：06:06:07
    当前时间为：06:30:39
    下一秒时间为：06:30:40
     */


    //    fun5()


//    val a = Stdln.readline()

  }

  /**
    * 需求：输入两个数，按从小到大排序后输出
    */
  def fun(a: Int, b: Int): Unit = {

    if (a > b) {
      println(b + " " + a)
    } else {
      println(a + " " + b)
    }

  }

  /**
    * 需求：输入三个数(互不相同)，并将三个升序输出(升序(从小到大))
    */
  def fun2(a: Int, b: Int, c: Int): Unit = {

    var (mx, mid, mi) = (0, 0, 0)

    var tmp = 0

    if (a > b) {
      if (a > c) {
        mx = a
        if (b > c) {
          mid = b
          mi = c
        } else {
          mid = c
          mi = b
        }
      } else {
        mx = c
        mid = a
        mi = b
      }
    } else {
      if (b > c) {
        mx = b
        if (a > c) {
          mid = a
          mi = c
        } else {
          mx = c
          mid = b
          mi = a
        }
      }
    }

    println(mi + " " + mid + " " + mx)

  }

  /**
    * 需求：输入一个年份，判断是否是闰年
    */
  def fun3(year: Int): Boolean = {

    if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)) {
      println(s"${year}是闰年")
      true
    } else {
      println(s"${year}不是闰年")
      false
    }

  }

  /**
    * 需求：输入一个时间，输出它的下一秒时间
    */
  def fun4(t: String): Unit = {

    println(s"当前时间为：${t}")

    val tArr = t.split(":")

    var (th, tm, ts) = (tArr(0).toInt, tArr(1).toInt, tArr(2).toInt)

    ts += 1

    if (ts >= 60) {
      ts = 0
      tm += 1
      if (tm >= 60) {
        tm = 0
        th += 1
        if (th >= 24) {
          th = 0
        }
      }
    }

    var (th2, tm2, ts2) = (th.toString, tm.toString, ts.toString)

    if (th2.length == 1) {
      th2 = "0" + th2
    }

    if (tm2.length == 1) {
      tm2 = "0" + tm2
    }

    if (ts2.length == 1) {
      ts2 = "0" + ts2
    }

    println("下一秒时间为：" + th2 + ":" + tm2 + ":" + ts2)

  }

  def fun4t(t: String): Unit = {
    val format = "HH:mm:ss"
//    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
//    val sjc: Long = sdf.parse(t).getTime + 1000
//    println(sdf.format(sjc))

    // 三行代码解决问题，多用已经写好的方法与函数（避免二次开发），加快开发速度
  }

  /**
    * 需求：输出1000以内所有的水仙花数，所谓水仙花数是指一个3位数，其各位数字立方和等于该数本身
    */
  def fun5(): Unit = {

    var (b, s, g) = (0, 0, 0)

    for (i <- 100 to 999) {
      b = i / 100
      s = (i % 100) / 10
      g = (i % 100) % 10

      if (b * b * b + s * s * s + g * g * g == i) {
        println(b + " " + s + " " + g)
        println(s"${i}是一个水仙花数")
      }
    }

  }
}

package com.al.scala.exam

/*
*孟祥琰
 */
object T2 {

  def main(args: Array[String]): Unit = {
    val h = new T2("mxy",23)

    println(h.name)
    println(h.age)
    println(h.jump)
    println(h.fight)

  }
}

trait Flyable {

  var distance: Int = 100
  def fight: String

}

abstract class Animal {

  var ages: Int
  var names: String
  def jump: String = "I can jump"
  def run
}


class T2(val name: String, var age: Int) extends Animal with Flyable {
  override var names: String = "m"
  override def jump: String = "I can jump high"
  override def run: Unit = "I can run"
  override var ages: Int = 19
  override def fight: String = "fighting"
  var gender: String = _
  def this(name: String, age: Int, gender: String) {
    // 辅助构造器方法的第一行代码必须先调用主构造器的字段
    this(name, age)
    this.gender = gender
  }
}



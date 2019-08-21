package com.al.scala.zuoye


import scala.collection.mutable
import scala.io.{BufferedSource, Source}


object zuoyeforthreeday {






  def main(args: Array[String]): Unit = {


    val arrnew = Array(1,2,3,2,4,1,5)
//    val rearr1 = getNum(arrnew,2)
//    println(rearr1.toBuffer)
//
//
//    val rearr2 = changeArr(arrnew)
//    rearr2.map(x => print(x))
//
//    val array: Array[Int] = getNumNew(arrnew, 2)
//
//    println(array.toBuffer)


//    val ints: Array[Int] = getNumNew2(arrnew,2)
//
//    ints.map(println(_))



    val list = List(("Java",1),("scala",2),("python",3))

    val tuple: (String, Int) = list.reduce((x, y) => (x._1 + y._1 , x._2 + y._2))

    print(tuple)


    val ints = list.map(x => x._2).sum
    println(ints)

    val content=Source.fromFile("d://data/test.txt","GBK").getLines()
    val lines=content.toList
    println("总行数：")
    println(lines.size)
    println("包含Spark的行数：")
    println(lines.filter(_.contains("Spark")).size)
    println(lines.count(_.contains("Spark")))
    println("文中的Spark个数：")
    println(
      lines.flatMap(_.split(" ")).filter(_.contains("Spark")).size
    )



//    valuesnew(x => x*x ,-5,5)

  }



  def getNum(arr : Array[Int],num : Int) : Array[Int] = {


//    val test1 =
     Array(arr.count(x => x < num),arr.count(x => x == num ), arr.count(x => x > num) )

//    test1
//    val arr1 = arr.filter(x => x < num)
//    val arr2 = arr.filter(x => x == num)
//    val arr3 = arr.filter(x => x > num)
//
//    val arrre = Array(arr1.length , arr2.length , arr3.length)



  }

  def getNumNew(array: Array[Int],int: Int):Array[Int] = {
    Array(array.count(x => x < int), array.count(x => x == int), array count (x => x > int))
  }

  def getNumNew2(array: Array[Int],int: Int):Array[Int] = {
    Array(array.partition(x => x < int)._1.length,array.partition(x => x < int)._2.partition(x => x == int)._1.length,array.partition(x => x < int)._2.partition(x => x == int)._2.length)
  }

  def changeArr(arr:Array[Int]): Array[Int] = {
    val len = arr.length
    var rearr = arr.map(x => x)
    var newlen = 0
    if(len%2 == 1){
      newlen = len - 1
    }else{
      newlen = len
    }
    for(x <- 0 until newlen) {
      if ((x+1) % 2 == 0) {
        rearr(x-1) = arr(x)
      }
      else {
        rearr(x+1) = arr(x)
      }
    }
    arr.map(print(_))
    println()
    rearr
  }


}

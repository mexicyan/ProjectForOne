package com.al.scala.day04

import scala.collection.{immutable, mutable}

object zuoye {



  def main(args: Array[String]): Unit = {


    val set: mutable.HashSet[(Int,Int)] = values(x => x*x , -5 , 5)

    val list: List[(Int,Int)] = set.toList.sortBy(_._1)


    val tuple: (Int, Int) = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).aggregate(0,0)((x, y) => (x._1+1 , x._2+y), (a, b) => (a._1+b._1,a._2 + b._2))

    println(tuple)


    val strings: List[String] = List("hello spark","hello scala","hello hadoop")


    val str: List[String] = strings.flatMap(_.split(" "))

    val map: List[(String, Int)] = str.map(x => (x, 1))

    val tuples: List[(String, (String, Int))] = map.map(x => (x._1, x))
    val map1 = tuples
    val reverse: Array[(String, Int)] = map1.groupBy(_._2).mapValues(_.size).toArray.sortBy(_._2).map(x=>(x._1._1,x._2))reverse

    println(reverse.toBuffer)



//   list.map(x => println(x.toBuffer))

    println(list)

    val ints: List[(Int,Int)] = valuesnew(x => x*x , -5 , 5)

    println(list)

  }


  def valuesnew(fun:(Int)=>Int,low:Int,high:Int) : List[(Int,Int)] = {

    val res = (for (i <- low to high) yield i).toList

    val list: List[(Int, Int)] = res.map(x => (x,fun(x)))
    list
  }


  def values(fun:(Int)=>Int,low:Int,high:Int): mutable.HashSet[(Int,Int)] ={

    val set = new mutable.HashSet[(Int,Int)]
    for(i <- low to high){
      set.add((i,fun(i)))
    }
    set
  }
}

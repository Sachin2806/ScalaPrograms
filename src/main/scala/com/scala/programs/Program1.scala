package com.scala.programs

import akka.Main
import org.apache.spark.SparkContext

object Program1 {
  
  def main(args: Array[String]){
    
    val sc = new SparkContext( "local", "Word Count", "/usr/local/spark", Nil, Map(), Map())     
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length)
    b.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Outfile")
  }
}
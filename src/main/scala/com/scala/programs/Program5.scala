package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions


object Program5 {
  
 def main(args: Array[String])
 {
   
  val conf = new SparkConf()
  .setAppName("Practice")
  .setMaster("local")

  val sc = new SparkContext(conf)
  
  val words = Array("one", "two", "two", "four", "five", "six", "six", "eight", "nine", "ten")
  val data = sc.parallelize(words).map(words => (words, 1)).reduceByKey(_+_)
  data.collect.foreach(println)
  data.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Practice_Output")
}
  
 }
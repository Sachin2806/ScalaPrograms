package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions


object Program2 {
  
  def main(args: Array[String]) = {

    //Create conf object
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
      
    //create spark context object
    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val test = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/wc.txt")

    val wc = test.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Outfile1")
    
    //Stop the Spark context
    sc.stop
  }
}


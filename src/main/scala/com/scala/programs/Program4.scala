package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions


object Program4 {
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Exercise").setMaster("local")
    val sc = new SparkContext(conf)
    
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _).collect()
    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum)).collect()
    
    println("Demo of reduceByKey")
    wordCountsWithReduce.foreach(println)
    
    println("Demo of groupByKey")
    wordCountsWithGroup.foreach(println)
    
  }
}
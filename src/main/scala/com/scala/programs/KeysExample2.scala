package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object KeysExample2 {
  
  def main(args: Array[String]){
    
    //set up spark context for local run
    val conf = new SparkConf()
       .setAppName("FileRead")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _).collect()    
    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum)).collect
    
    wordCountsWithReduce.foreach(println)
    wordCountsWithGroup.foreach(println)
  }
}

package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object KeysExample {
  
  def main(args: Array[String]){
    
    //set up spark context for local run
    val conf = new SparkConf()
       .setAppName("FileRead")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    val soccer = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/Input/soccer.txt")
    
    //Converting data to a tuple, by splitting at delimiter. Score converted to a number explicitly
    val myPair=soccer.map{k => (k.split(" ")(0),k.split(" ")(1).toInt)}
    
    myPair.foreach(println)
    myPair.groupByKey().foreach(println)
    myPair.groupByKey().mapValues { x => x.reduce((a,b)=>a + b ) }.foreach(println)
    myPair.groupByKey().map { x => (x._1, x._2.sum) }.foreach(println)
    myPair.reduceByKey { case (a, b) => a + b }.foreach { println }
        
}
}

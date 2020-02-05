package com.scala.programs

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object WordCount {
  
  def main(args: Array[String]){
    
      val sc = new SparkContext( "local", "Word Count", "/usr/local/spark", Nil, Map(), Map())           
      val input = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/wc.txt")             
      val count = input.flatMap(line ⇒ line.split(" ")).map(word ⇒ (word, 1)).reduceByKey(_ + _)       
      count.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Outfile")
      System.out.println("OK");
  }
}
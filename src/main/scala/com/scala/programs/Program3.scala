package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Program3 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("BabyNames")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    val babyNames = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/Input/Baby_Names__Beginning_2007.csv")
    val rows = babyNames.map(line => line.split(","))
    
    val sampleRDD = sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect
    println(sampleRDD)

    sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect
  }
}
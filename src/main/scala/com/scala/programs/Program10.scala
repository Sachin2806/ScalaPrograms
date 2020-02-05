package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Program10 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("BabyNames")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    val fileRDD = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/Input/Spark.txt")
    val flatMapRDD = fileRDD.flatMap(line => line.split(" "))
    val mapRDD = flatMapRDD.map(w => (w,1))
    val keyRDD = mapRDD.reduceByKey(_+_)
    keyRDD.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Spark_Output")  
    
  }
}
package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Program9 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("BabyNames")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    val babyNames = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/Input/Baby_Names__Beginning_2007.csv")
    val rows = babyNames.map(line => line.split(","))
    val namesToCounties = rows.map(name => (name(1),name(2)))
    namesToCounties.groupByKey.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Practice_Output")
    val filteredRows = babyNames.filter(line => !line.contains("Count")).map(line => line.split(","))
    filteredRows.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Practice_Output")
    
    
    


  }
}
package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Program6 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("BabyNames")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    val babyNames = sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/Input/Baby_Names__Beginning_2007.csv")
     
// Count - Displays the total count in a file
   println("Total no of records", babyNames.count)
	
// Displays the first row from the file
    println("Total no of records", babyNames.first())
	
// To find unique rows from the file
	val rows = babyNames.map(line => line.split(","))
	val uniqueRows = rows.map(row => row(2)).distinct.count
	
	println(uniqueRows)
  }
}
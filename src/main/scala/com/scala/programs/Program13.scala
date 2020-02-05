package com.scala.programs



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Program13 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("FileRead")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    //Demo of Basic Actions
    val rdd = sc.parallelize(List(1, 2, 3, 3, 4))
    //val rdd2 = sc.parallelize(List(3, 4, 5))
    
    rdd.collect.mkString(",").foreach(print)
    println()
    println("Count : " + rdd.count())   
       
    println("Count By Value : " + rdd.countByValue())
        
    rdd.take(3).mkString(",").foreach(print)
    println()
    
    rdd.top(3).mkString(",").foreach(print)
    println()
    
    rdd.takeOrdered(3).mkString(",").foreach(print)
    println()
    
    println("Count with Reduce : " + rdd.reduce((x, y) => x + y))
    println("Count with Fold : " + rdd.fold(0)((x, y) => x + y))
        
}
}

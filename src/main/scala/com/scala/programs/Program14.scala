package com.scala.programs



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Program14 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("FileRead")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    //Demo of Basic Actions
    val s = sc.parallelize(List(1, 2, 3, 4))
       
    val r = s.aggregate((0, 0))((s, r) =>(s._1 + r, s._2 + 1),  
                                (s,r) => (s._1 + r._1, s._2 + r._2)) 
     
    // Displays summation of all the elements in the list and also total number of elements 
    println("(Sum of all the elements , total number of elements) = " + r) 
        
}
}

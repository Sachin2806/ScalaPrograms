package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Program12 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("FileRead")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    //Demo of Transformations
    val rdd1 = sc.parallelize(List(1, 2, 3, 4))
    val rdd2 = sc.parallelize(List(3, 4, 5))
    
    val unionRDD = rdd1.union(rdd2)
    val subtractRDD = rdd1.subtract(rdd2)
        
    //unionRDD.collect.mkString(",").foreach(print)
    rdd1.union(rdd2).collect.mkString(",").foreach(print)
    rdd1.subtract(rdd2).collect.mkString(",").foreach(print)
    
    
    
    
  }
}
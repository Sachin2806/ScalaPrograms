package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object Program11 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
       .setAppName("FileRead")
       .setMaster("local")
       
    val sc = new SparkContext(conf)
    
    //Demo of Actions
    val rdd = sc.parallelize(List(1, 2, 3, 3))
    val mapRDD = rdd.map(x => x + 1)
    val flatMapRDD1 = rdd.flatMap(x => x.to(1))
    val flatMapRDD2 = rdd.flatMap(x => x.to(2))
    val flatMapRDD3 = rdd.flatMap(x => x.to(3))
    val flatMapRDD4 = rdd.flatMap(x => x.to(4))
    
    val filterRDD = rdd.filter(x => x != 1)
    
    mapRDD.take(4).mkString(",").foreach(print)
    flatMapRDD1.collect.mkString(",").foreach(print)  
    flatMapRDD2.collect.mkString(",").foreach(print)  
    flatMapRDD3.collect.mkString(",").foreach(print)  
    flatMapRDD4.collect.mkString(",").foreach(print)
    
    filterRDD.collect.mkString(",").foreach(print)
    
    
  }
}
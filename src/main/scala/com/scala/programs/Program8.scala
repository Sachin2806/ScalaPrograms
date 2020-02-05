package com.scala.programs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.api.java.JavaSparkContext;

object Program8 {
  
 def main(args: Array[String]){
   
 try
 {
 val conf = new SparkConf().setAppName("BabyNames1").setMaster("local")       
 val sc = new SparkContext(conf)
    
// Define RDD with raw data
val rawData=sc.textFile("C:/Users/CSC/workspace/ScalaDemo/Files/Input/Cars2.txt")
rawData.take(0).foreach(println)
println("Raw Data", rawData)

// map columns and datatypes
case class cars(make:String,Model:String,MPG:String)

val carsData=rawData.map(x=>x.split("\t"))
println(carsData)

val carsRDD = carsData.map(x=>cars(x(0).toString,x(1).toString,x(2).toString))
println(carsRDD)

carsRDD.take(2)

//persist to memory
carsRDD.cache()
    }catch {
  case e: Exception => println("exception caught: " + e);
    
    }
////count cars origin wise
//carsData.map(x=>(x.origin,1)).reduceByKey((x,y)=>x+y).collect
//
////filter out american cars
//val americanCars=carsData.filter(x=>(x.origin=="American"))
//
////count total american cars
//americanCars.count()

//take sum of weights according to make
//val makeWeightSum=americanCars.map(x=>(x.make,x.weight.toInt)).combineByKey((x:Int) => (x, 1),
//(acc:(Int, Int), x) => (acc._1 + x, acc._2 + 1),                                                                                                                
//(acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
//makeWeightSum.collect()
//
//
////take average
//val makeWeightAvg=makeWeightSum.map(x=>(x._1,(x._2._1/x._2._2)))
//makeWeightAvg.collect()
//
//
////save output to disc/HDFS
//makeWeightAvg.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Output/carsMakeWeightAvg.txt")
}
}


package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object WordCount {
  def main(args:Array[String]) {  
 

val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
val sc = new SparkContext(conf)

val textFile = sc.textFile("C:/spark-2.1.1-bin-hadoop2.7/spark-2.1.1-bin-hadoop2.7/license")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("NewData")

println("Total number of lines in file: "+ textFile.count())
println("Hello Spark");

}

}
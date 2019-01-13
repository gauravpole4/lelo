package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object BankCustomers {
  
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "UK BANK Customers")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("C:/Users/niles/Documents/SparkWorkSpace/ScalaExamples/src/com/sundogsoftware/sparkstreaming/UK-Bank-Customers-Noheader.csv")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split(",")(4))
    
    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
    results.foreach(println)
 }
}
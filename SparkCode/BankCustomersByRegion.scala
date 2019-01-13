package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

 
  object BankCustomersByRegion {
  
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val region = fields(5)
      
      // Create a tuple that is our result.
      (region)
  }
  
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "UK BANK Customers")
   
    // Load up each line of the  data into an RDD
    val lines = sc.textFile("C:/Users/niles/Documents/SparkWorkSpace/ScalaExamples/src/com/sundogsoftware/sparkstreaming/UK-Bank-Customers-Noheader.csv")
    
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    val results = rdd.countByValue()
    results.toArray.sorted.foreach(println)
    
    
 }
}
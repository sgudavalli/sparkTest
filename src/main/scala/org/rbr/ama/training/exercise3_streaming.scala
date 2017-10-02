package org.rbr.ama.training



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object exercise3_streaming {
  
  
  def main(args: Array[String]) {

    val logFile = "file:///Users/sivagudavalli/spark/data/amadeustraining/training_materials/sparkdev/data/weblogs_streaming"
    val checkpointDir = "_checkpoint"
    
    
    println("App started")

    val conf = new SparkConf().setAppName("sparkexercise2")
    
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchDuration = Seconds(2))
    
    
    val lines = ssc.textFileStream(logFile)
    lines.filter( _.contains("KBDOC"))
          .foreachRDD(rdd => println("Number of KB requests: " 
              + rdd.id + "->" 
              + rdd.name + "->" 
              + rdd.count()))
//          
    ssc.start()
    ssc.awaitTermination()
    
  }
  
  
}
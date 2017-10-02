package org.rbr.ama.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import scala.xml._
import org.apache.spark.storage.StorageLevel

object exercise2_checkpointing {
  
  def main(args: Array[String]) {

    println("App started")

    val conf = new SparkConf().setAppName("sparkexercise2").setMaster("local")
    
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))
    
    val rdd = sc.parallelize(0 to 9)
    val cis = new dstream.ConstantInputDStream(ssc, rdd)
    cis.print()
    
    val checkpointDir = "_checkpoint"
    ssc.checkpoint(checkpointDir)
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}
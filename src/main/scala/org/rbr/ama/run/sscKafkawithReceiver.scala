package org.rbr.ama.run

import org.apache.spark.SparkConf 
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils 

object kafkawithReceiver {
  
  def main(args : Array[String]) {
    
    var numberOftweets=0
    val conf = new SparkConf()
      .setAppName("sparkStreamingApp")
      .setMaster("local[2]")
      
    val ssc = new StreamingContext(conf, Seconds(60))
    
    val Array(zkQuorum, group, topics, numThreads) = Array("localhost:2181", "sscKafkaTest", "trump,sanders", "3")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    
    messages.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          partitionofRecords => {
            partitionofRecords.foreach (
              row => 
                { 
                  println("============================")
                  println(row)
                  numberOftweets = numberOftweets+1                  
                }
            )
          }
        )
      }
    )
    
    ssc.start()
    ssc.awaitTermination()
    
    println ("total number of tweets processed " + numberOftweets)
    println("============================")
    
  }
  
  
}
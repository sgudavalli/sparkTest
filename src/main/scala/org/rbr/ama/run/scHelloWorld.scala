package org.rbr.ama.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * @author --> sgudavalli
 */
object scHelloWorld {

  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("sparkApp")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val col = sc.parallelize(0 to 100 by 5)
    // col.collect().foreach(println)
    val smp = col.sample(true, 4)   
    val colCount = col.count
    val smpCount = smp.count
    
    println("orig count = " + colCount)
    println("sampled count = " + smpCount)
  }

}

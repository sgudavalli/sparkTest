/**
 * Created by sivagudavalli on 9/29/15.
 */
package org.rbr.ama.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object sparkApp {
  def main(args: Array[String]) {

    println("App started")

    val logFile = "/Users/sivagudavalli/spark/README.md"
    val conf = new SparkConf().setAppName("WC_sparkApp").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

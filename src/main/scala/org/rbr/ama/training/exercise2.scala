package org.rbr.ama.training


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml._
import org.apache.spark.storage.StorageLevel

object exercise2 {
  
  def getactivations(fileiterator: Iterator[String]): Iterator[Node] = {
    val nodes = XML.loadString(fileiterator.mkString) \\ "activation"
    nodes.toIterator
  }
  
  def getmodel(activation: Node): String = {
    (activation \ "model").text
  }
  
  def main(args: Array[String]) {

    println("App started")

    val activations = "/user/sivagudavalli/activations/*"
    
    val conf = new SparkConf().setAppName("sparkexercise2")
    val sc = new SparkContext(conf)
    
    val jpcounts = sc.textFile(activations)
                    .mapPartitions(getactivations(_))
                    .map(x => (getmodel(x),1 ))
                    .reduceByKey( (a,b) => (a+b))
                    .map(x => x.swap)
                    .sortByKey(false)
                    .take(5)
                    .map(println)
                    
                       
                    
    // cache(), 
    // unpersist() & 
    // persist(StorageLevel.DISK_ONLY)
    
    
  }
  
}
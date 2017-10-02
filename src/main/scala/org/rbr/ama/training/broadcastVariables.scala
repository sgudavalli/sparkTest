package org.rbr.ama.training

import scala.xml._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object broadcastVariables {
  
  def getactivations(fileiterator: Iterator[String]): Iterator[Node] = {
    val nodes = XML.loadString(fileiterator.mkString) \\ "activation"
    nodes.toIterator
  }
  
  def getmodel(activation: Node): String = {
    (activation \ "model").text
  }
  
  def main(args: Array[String]) {
    
//    import scala.io.Source
//    val targetfile = "/user/sivagudavalli/targetmodels.txt"
//    val targetlist = Source.fromFile(targetfile)
//                            .getLines.map(_.split(" "))
//                            .map(strings => (strings(0),strings(1)))
//                            .toMap
           
    val conf = new SparkConf().setAppName("broadcastVariables")                        
    val sc = new SparkContext(conf)
//    val targetModels = sc.broadcast(targetlist)
    
    val ssc = new StreamingContext(sc, batchDuration = Seconds(2))
    
    val logFile = "file:///Users/sivagudavalli/spark/data/amadeustraining/training_materials/sparkdev/data/weblogs_streaming"
    val lines = ssc.textFileStream(logFile).foreachRDD{
      x => x.mapPartitions(getactivations(_))
            .map(x => (getmodel(x),1 ))
//            .map(x => (targetModels.value(x._1), x._2))
            .map(println)
    }
    
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
}
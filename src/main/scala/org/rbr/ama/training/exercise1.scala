package org.rbr.ama.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object exercise1 {
  def main(args: Array[String]) {

    println("App started")

    val logFile = "/Users/sivagudavalli/spark/data/amadeustraining/training_materials/sparkdev/data/weblogs/*"
    val accounts = "/Users/sivagudavalli/spark/data/amadeustraining/training_materials/sparkdev/data/accounts.csv"
    
    val conf = new SparkConf().setAppName("sparkexercise1").setMaster("local")
    val sc = new SparkContext(conf)
    
    /*
    
    val jpglines = sc.textFile(logFile)
                      .filter(_.contains(".jpg"))
//                      .map(_.length)
                        .map(_.split(" ")(0))
//                      .map(x => x.split(" ").toList)
                      .saveAsTextFile("/Users/sivagudavalli/spark/data/amadeustraining/"
                          + "training_materials/sparkdev/data/exercise1_jpg_ip_address")
                     
    val userIdhitCounts = sc.textFile(logFile)
                              .filter(_.contains(".htm"))
                              .map(_.split(" "))
                              .map(x => (x(2),1))
                              .reduceByKey((a,b) => a+b)
                              .map( x => x.swap )
                              .sortByKey(false)
    
    val htmpairRdds = sc.textFile(logFile)
                          .filter(_.contains(".htm")) 
                          .map(_.split(" "))
                          .map(x => (x(2), x(0)))
                          .groupByKey()
                          .take(5)
                          .map(println)
     
     */
    
     /* 
     
     val userIdhitCounts = sc.textFile(logFile)
                              .filter(_.contains(".htm"))
                              .map(_.split(" "))
                              .map(x => (x(2),1))
                              .reduceByKey((a,b) => a+b)
                              
    
     val accountsRdd = sc.textFile(accounts)
                          .map(_.split(","))
                          .map(x => (x(0), x))
                          
                          
     val joinuserIdhitCountswithAccounts = accountsRdd
                                             .join (userIdhitCounts)
                                             .map(x => (x._1, x._2._2, x._2._1(3), x._2._1(4)))
                                             .take(10)
                                             .map(println)
      
      */
      
    val accountsRdd = sc.textFile(accounts)
                           .keyBy(_.split(",")(8))
                           .map(x => (x._1, x._2.split(",")) )
                           .map( x => (x._1,  (x._2(3), x._2(4) ) ) )
                           .groupByKey
                           .sortByKey(true)
                           .take(5)
                           .map(println)
                          
    
  }
  
  
}
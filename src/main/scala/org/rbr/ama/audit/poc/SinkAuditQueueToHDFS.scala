package org.rbr.ama.audit.poc


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.serializer.StringDecoder
import java.text.SimpleDateFormat

import org.json4s.JsonAST.JField
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hdfs.server.namenode.SafeMode
import org.apache.spark.sql.SaveMode


object SinkAuditQueueToHDFS {
  
  def main(args: Array[String]) {
    
    val topicsIn = "audit";
    val batchsizeInSec = 30
    val checkpointDir = "hdfs://localhost:9000/apps/spark/chkpointdir4"
    val dest = "hdfs://localhost:9000/apps/spark/audit4"
    
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092",

      // Number of individual task failures before giving up on the job. 
      "spark.task.maxFailures" -> "1",

      // If set to "true", performs speculative execution of tasks. 
      // This means if one or more tasks are running slowly in a stage, they will be re-launched.
      "spark.speculation" -> "false",

      // smallest : automatically reset the offset to the smallest offset
      // largest : automatically reset the offset to the largest offset
      "auto.offset.reset" -> "largest")
      
      
      val conf = new SparkConf().setMaster("local[6]").setAppName("auditSink")
      val context = StreamingContext.getOrCreate(checkpointDir,
      () => {
        functionToCreateContext(conf, kafkaParams, checkpointDir, topicsIn, batchsizeInSec, dest)
      })

      context.start()
      context.awaitTermination()
  }
  
  object SQLContextSingleton {
        @transient  private var instance: SQLContext = _
        def getInstance(sparkContext: SparkContext): SQLContext = {
                if (instance == null) {
                        instance = new SQLContext(sparkContext)
                }
                instance
        }
  }
  
  def functionToCreateContext(sparkConf: SparkConf, kafkaParams: Map[String, String],
                              checkpointDirectory: String, topicsIn: String, 
                              batchsizeInSec: Long, dest: String): StreamingContext = {

    val ssc = new StreamingContext(sparkConf, Seconds(batchsizeInSec))
    
    ssc.checkpoint(checkpointDirectory)
    val topics = topicsIn.split(",").toSet

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    
    stream.foreachRDD { rdd => 
      
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      
      val df = rdd.mapPartitionsWithIndex{ (i,iter) => 
        val osr: OffsetRange = offsetRanges(i)
        val prefix  = osr.topic +":" + osr.partition + " =>" + osr.fromOffset + " -> " + osr.untilOffset
        println (prefix)
        iter.toList.map(x => AuditMapFunction(x._2)).iterator
      }.map(x => x._2).toDF()
          
      df.write.mode(SaveMode.Append)
          .partitionBy("creationDate")
          .format("parquet")
          .save(dest)
    } 
    ssc
  
  }
  
  def AuditMapFunction  ( row :String ) : (String, Record) = {
      implicit val formats = DefaultFormats
      val json = parse(row)
      val audit = json.extract[ Audit ]
      
      val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val timeFormat = new SimpleDateFormat("HHmmss")

      
      val rc  = Record (audit.data(0).id, 
          
          audit.data(0).scope.chainCode, 
          audit.data(0).scope.propertyCode, 
          audit.data(0).scope.brandCode, 
          audit.data(0).scope.regionCode, 
          
          audit.data(0).properties.category,
          audit.data(0).properties.subCategory, 
          
          audit.header.security.userId, 
          audit.header.security.organization,
          
          audit.data(0).properties.action, 
          
          dateFormat.format(inputFormat.parse(audit.data(0).lastModified)), 
          timeFormat.format(inputFormat.parse(audit.data(0).lastModified)), 
          
          audit.data(0).properties.codes(0),
          audit.data(0).properties.codes(1),
          audit.data(0).properties.codes(2),
          
          audit.data(0).properties.dates(0),
          audit.data(0).properties.dates(1)
      )
      
      (row, rc)
  }
  
  case class Record(id: String, chain: String, hotel: String, brand: Option[String], region: Option[String],
      category: String, subCategory: String, userId: String, organization: String, 
      action: String, creationDate: String, creationTime: String,
      code1: String, code2: String, code3: String, date1: String, date2: String)
      
  case class Properties (action: String, codes: List[String], 
      category: String, subCategory: String, dates: List[String])
  case class Scope(chainCode: String, propertyCode: String, brandCode: Option[String], 
      regionCode: Option[String])
  case class Data(id: String, lastModified: String, properties: Properties, scope: Scope)
  case class Audit(data: List[Data], header: Header)
  case class Header(security: Security)
  case class Security(userId: String, organization: String)
  
  
}
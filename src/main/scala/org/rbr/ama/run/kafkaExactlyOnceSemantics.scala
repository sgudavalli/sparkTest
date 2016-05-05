package org.rbr.ama.run

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.serializer.StringDecoder
import org.apache.spark.TaskContext
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.sql.SQLContext
import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.streaming.Minutes
import java.util.Calendar
import java.util.GregorianCalendar
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.mongodb.casbah.{ WriteConcern => MongodbWriteConcern }
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.Path
import java.io.PrintWriter;



//case class Twitter (topic: String, partition: String, fromOffSet: String, toOffSet: String, metrics: List[Metric])
//case class Metric (lang: String, count: Int)

object kafkaExactlyOnceSemantics {

  private val twitterDateTime = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")

  def safeDateTime(created_at: String): Timestamp = {
    Try(new Timestamp(twitterDateTime.parse(created_at).getTime)).getOrElse(null)
  }
  
  def functionToCreateContext(sparkConf: SparkConf, kafkaParams: Map[String, String],
                              checkpointDirectory: String, topicsIn: String, batchsizeInSec: Long, mongoWriteConfig: MongodbConfigBuilder): StreamingContext = {

    val ssc = new StreamingContext(sparkConf, Seconds(batchsizeInSec))
    ssc.checkpoint(checkpointDirectory)

    val topics = topicsIn.split(",").toSet

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val loadhdfs = "hdfs://localhost:9000/apps/spark/loadhdfs"
    
    
    stream.foreachRDD { rdd => 
      
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      
      rdd.mapPartitionsWithIndex{ (i,iter) => 
        val osr: OffsetRange = offsetRanges(i)
        Tweeter (i,iter, osr)
      }  .coalesce(6, false). aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(println)
    } 
    
    /*stream.foreachRDD { rdd =>

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      if (rdd.toLocalIterator.nonEmpty) {

        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        sqlContext.read.json(rdd.map(_._2)).registerTempTable("mytable")
        
        rdd.foreachPartition  { iter =>

          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)  
          println ( "you are READING "  + o.topic +":" + o.partition + " =>" + o.fromOffset + " -> " + o.untilOffset)
          iter.map(row => TweetMapFunction(row._2)).foreach(println)
          
        }

      }

    }*/

    ssc
  }
  
  def Tweeter (  index: Int, iter: Iterator[(String, String)], o: OffsetRange ) : Iterator [(String, Integer)] = {
   
    val prefix  = o.topic +":" + o.partition + " =>" + o.fromOffset + " -> " + o.untilOffset
    iter.toList.map( x =>   TweetMapFunction(x._2) ).map( x => ( prefix + " : " + x._1, x._2) ). iterator
  }
  
  def TweetMapFunction  ( row :String ) : (String, Integer) = {
     val xyz = scala.util.parsing.json.JSON.parseFull(row)
     val globalMap = xyz.get.asInstanceOf[Map[String, Any]]
     val user = globalMap.get("user").get.asInstanceOf[Map[String, Any]]
     (user.get("screen_name").get.toString() , 1)
  }

  def main(args: Array[String]) {

    val tweetsDB = MongodbConfigBuilder(Map(Host -> List("localhost:27017"),
      Database -> "tweetsDB", Collection -> "tweets"))

    val checkpointDir = "hdfs://localhost:9000/apps/spark/chkpointdir3"
    
    
    val topicsIn = "tweets"
    val batchsizeInSec = 10

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094",

      // Number of individual task failures before giving up on the job. 
      "spark.task.maxFailures" -> "1",

      // If set to "true", performs speculative execution of tasks. 
      // This means if one or more tasks are running slowly in a stage, they will be re-launched.
      "spark.speculation" -> "false",

      // smallest : automatically reset the offset to the smallest offset
      // largest : automatically reset the offset to the largest offset
      "auto.offset.reset" -> "smallest")

    val conf = new SparkConf()
      .setMaster("local[6]")
      .setAppName("sparkStreamingApp")

    val context = StreamingContext.getOrCreate(checkpointDir,
      () => {
        functionToCreateContext(conf, kafkaParams, checkpointDir, topicsIn, batchsizeInSec, tweetsDB)
      })

    context.start()

    context.awaitTermination()

  }

}
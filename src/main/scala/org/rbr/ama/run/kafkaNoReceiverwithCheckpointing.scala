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
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import org.apache.spark.sql.SaveMode

object kafkaNoReceiverwithCheckpointing {

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

    stream.foreachRDD { rdd =>

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      sqlContext.udf.register("reportTime", (created_at: String) =>
        {
          val createdAt = safeDateTime(created_at)
          val calendar = new GregorianCalendar
          calendar.setTimeInMillis(createdAt.getTime)

          val dateuntilHour = calendar.get(Calendar.YEAR) + "-" +
            "%02d".format(calendar.get(Calendar.MONTH)) + "-" +
            "%02d".format(calendar.get(Calendar.DAY_OF_MONTH)) + " " +
            "%02d".format(calendar.get(Calendar.HOUR_OF_DAY)) + ":"

          calendar.get(Calendar.MINUTE) match {
            case x if x >= 0 && x < 9   => dateuntilHour + "00"
            case x if x >= 10 && x < 19 => dateuntilHour + "10"
            case x if x >= 20 && x < 29 => dateuntilHour + "20"
            case x if x >= 30 && x < 39 => dateuntilHour + "30"
            case x if x >= 40 && x < 49 => dateuntilHour + "40"
            case x if x >= 50 && x < 59 => dateuntilHour + "50"
          }
        })

      if (rdd.toLocalIterator.nonEmpty) {
        rdd.foreachPartition { iter =>
              val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
              println(s"topic ${o.topic} partition ${o.partition} from-offset ${o.fromOffset} to-offset ${o.untilOffset}")        
        }
        
        sqlContext.read.json(rdd.map(_._2)).registerTempTable("mytable")
        
        // val tweets = sqlContext.sql("select reportTime(created_at) dim_, id, text " +
          // "from mytable")
        val tweets = sqlContext.sql("select * from mytable")
        
        val writeConfig = mongoWriteConfig.build()
        // val df = tweets.groupBy("dim_").count().toDF.saveToMongodb(writeConfig, true)
        val df = tweets.saveToMongodb(writeConfig, true)

      }

    }

    ssc
  }

  def main(args: Array[String]) {

    val tweetsDB = MongodbConfigBuilder(Map(Host -> List("192.168.0.4:27017"), 
                                    Database -> "tweetsDB", Collection -> "tweets"))
    
    val checkpointDir = "hdfs://localhost:9000/apps/spark/chkpointdir"
    val topicsIn = args(0).toString
    val batchsizeInSec = 10

    val kafkaParams = Map("metadata.broker.list" -> "192.168.0.4:9092,192.168.0.4:9093,192.168.0.4:9094",

      // Number of individual task failures before giving up on the job. 
      "spark.task.maxFailures" -> "1",

      // If set to "true", performs speculative execution of tasks. 
      // This means if one or more tasks are running slowly in a stage, they will be re-launched.
      "spark.speculation" -> "false",

      // smallest : automatically reset the offset to the smallest offset
      // largest : automatically reset the offset to the largest offset
      "auto.offset.reset" -> "smallest")

    val conf = new SparkConf()
//      .setMaster("local[2]")
      .setAppName("sparkStreamingApp")

    val context = StreamingContext.getOrCreate(checkpointDir,
      () => {
        functionToCreateContext(conf, kafkaParams, checkpointDir, topicsIn, batchsizeInSec, tweetsDB)
      })

    context.start()

    context.awaitTermination()

  }

}
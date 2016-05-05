package org.rbr.ama.run

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.serializer.StringDecoder

object kafkaNoReceiver {

  def main(args: Array[String]) {

    var numberOftweets = 0
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("sparkStreamingApp")

    val ssc = new StreamingContext (conf, Seconds(30))
    ssc.checkpoint("hdfs://localhost:9000/apps/spark/chkpointdir")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.0.5:9092,192.168.0.5:9093,192.168.0.5:9094",

      // Number of individual task failures before giving up on the job. 
      "spark.task.maxFailures" -> "1",

      // If set to "true", performs speculative execution of tasks. 
      // This means if one or more tasks are running slowly in a stage, they will be re-launched.
      "spark.speculation" -> "false",

      // smallest : automatically reset the offset to the smallest offset
      // largest : automatically reset the offset to the largest offset
      "auto.offset.reset" -> "smallest")

    val topics = Set("trump")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    var offsetRanges = Array[OffsetRange]()

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

      }

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
/**
 * Created by sivagudavalli on 9/29/15.
 */

package org.rbr.ama.run

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

/**
 * @author sivagudavalli
 */
object streamingApp {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
                          .setAppName("WC_streamingApp")
                          .setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("/usr/local/sparkInput")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    print("-----------------")
    wordCounts.print()
    print("-----------------")

    ssc.start()
    ssc.awaitTermination()
  }
}


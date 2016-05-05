package org.rbr.ama.run

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties
import org.apache.spark.sql.{Row, SaveMode}

object sscSQLLoad {
  
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("WC_streamingKafkaApp").setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("/usr/local/sparkInput")
    val words = lines.flatMap(_.split(" "))

    // **** Start ====> mysql connection specific details
    val user = "postgres"
    val pwd = "postgres"
    val url = "jdbc:postgresql://localhost:5432/hivedb"

    val prop = new java.util.Properties
    prop.setProperty("user","postgres")
    prop.setProperty("password","postgres")
    // **** End ====> mysql connection specific details

    words.foreachRDD {
      rdd =>
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)

        import sqlContext.implicits._

        val wordsDataFrame = rdd.toDF("word")

        wordsDataFrame.registerTempTable("words")

        val wordCountsDataFrame =
          sqlContext.sql("select word, count(*) as total from words group by word")

        wordCountsDataFrame.write.mode(SaveMode.Append).jdbc(url, "wordcounts", prop)

        // wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
  
}
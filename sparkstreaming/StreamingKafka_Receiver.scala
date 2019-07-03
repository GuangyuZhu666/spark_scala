package com.spark.test.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming整合KafKa,使用第一种receiver方式
  */
object StreamingKafka_Receiver {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("StreamingKafka_Receiver").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    streamingContext.checkpoint("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkstreaming/checkpoints03/")

    val config = Map("flume-kafka" -> 2)
    val lineDStream: DStream[String] = KafkaUtils.createStream(streamingContext,
      "master,slave1,slave2",
      "SK_R",
      config,
      StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

    val wordAndCountDStream = lineDStream.flatMap(_.split(" ")).map(word => (word, 1)).updateStateByKey(
      (newValues: Seq[Int], state: Option[Int]) => {
        Some(newValues.sum + state.getOrElse(0))
      }
    )

    wordAndCountDStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}

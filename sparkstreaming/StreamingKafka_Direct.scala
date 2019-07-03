package com.spark.test.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingKafka_Direct {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("StreamingKafka_Direct").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    streamingContext.checkpoint("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkstreaming/checkpoints04/")

    val kafkaParams = Map("metadata.broker.list" -> "master:9092,slave1:9092,slave2:9092")
    val lineDStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext,
      kafkaParams,
      Set("flume-kafka")).map(_._2)

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

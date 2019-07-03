package com.spark.test.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("NetWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.43.218", 9999)

    // 读HDFS的
    // streamingContext.textFileStream("hdfs://")

    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val wordsAndOneDStream: DStream[(String, Int)] = wordsDStream.map(word => (word, 1))

    // 无状态计算，每次都只输出当前时间片段的DStream的计算结果
    val wordsAndCounts: DStream[(String, Int)] = wordsAndOneDStream.reduceByKey((x, y) => x+y)

    wordsAndCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}

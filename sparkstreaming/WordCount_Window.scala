package com.spark.test.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCount_Window {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("NetWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    streamingContext.checkpoint("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkstreaming/checkpoints02/")

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.43.218", 9999)

    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))

    val wordsAndOneDStream: DStream[(String, Int)] = wordsDStream.map(word => (word, 1))

    /**
      * 窗口操作:每隔4s计算过去6s的数据（注意：必须是要时间片长度的倍数）
      * Func
      * 窗口长度
      * 滑动周期
      */
    val wordsAndCounts: DStream[(String, Int)] = wordsAndOneDStream.reduceByKeyAndWindow(
      (x:Int, y:Int) => x+y,
      Seconds(6),
      Seconds(4)
    )

    wordsAndCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}

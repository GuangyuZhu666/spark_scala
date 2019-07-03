package com.spark.test.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 高可用，即使程序挂掉后再启动，依旧从checkpoins中恢复streamingcontext
  */
object WordCount_DriverHA {

  def main(args: Array[String]): Unit = {

    val checkpointDirectory = "/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkstreaming/checkpoints/"

    def functionToCreateContext(): StreamingContext = {

      val sparkConf: SparkConf = new SparkConf().setAppName("NetWordCount").setMaster("local[2]")
      val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

      val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.43.218", 9999)
      val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
      val wordsAndOneDStream: DStream[(String, Int)] = wordsDStream.map(word => (word, 1))

      def updateFunction(newValues : Seq[Int], state : Option[Int]) : Option[Int] ={
        val new_value = newValues.sum
        val state_value = state.getOrElse(0)
        Some(new_value + state_value)
      }

      val wordsAndCounts: DStream[(String, Int)] = wordsAndOneDStream.updateStateByKey(updateFunction)
      wordsAndCounts.print()

      streamingContext.checkpoint(checkpointDirectory)   // set checkpoint directory
      streamingContext.start()
      streamingContext.awaitTermination()
      streamingContext
    }

    // Get StreamingContext from checkpoint data or create a new one
    val streamingContext = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    streamingContext.start()
    streamingContext.awaitTermination()

  }


}

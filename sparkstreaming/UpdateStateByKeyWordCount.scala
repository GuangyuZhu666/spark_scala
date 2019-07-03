package com.spark.test.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object UpdateStateByKeyWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("NetWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    /**
      * Exception: the checkpoint has not been set
      * 需要一个中间目录去保存中间结果（hdfs）
     */
    streamingContext.checkpoint("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkstreaming/checkpoints/")

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.43.218", 9999)

    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val wordsAndOneDStream: DStream[(String, Int)] = wordsDStream.map(word => (word, 1))

    /**
      * newValues : Seq[Int]  ,  state : Option[S]) =>  Option[S]
      * 传过来的新值序列             旧状态的值         =>  合并成新状态的值
     */
    def updateFunction(newValues : Seq[Int], state : Option[Int]) : Option[Int] ={
      val new_value = newValues.sum
      val state_value = state.getOrElse(0)
      Some(new_value + state_value)
    }

    /**
      * updateStateByKey就是拿当前传过来的某个key的新values序列，和之前该key已经累计出来的结果合并计算，得到新结果
      */
    val wordsAndCounts: DStream[(String, Int)] = wordsAndOneDStream.updateStateByKey(updateFunction)

    wordsAndCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

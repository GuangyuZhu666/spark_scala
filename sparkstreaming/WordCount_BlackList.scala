package com.spark.test.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * transform操作
  * 黑名单过滤案例
  */
object WordCount_BlackList {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("NetWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    streamingContext.checkpoint("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkstreaming/checkpoints01/")

    val blackList = List(",", ".", "!", "?", "#", "@", "%", "&")
    val blackListBC = streamingContext.sparkContext.broadcast(blackList)

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.43.218", 9999)
    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))

    val transformFunc : RDD[String] => RDD[String] = (rdd:RDD[String]) => {
      val newRdd: RDD[String] = rdd.mapPartitions(ptnData => {
        val bl = blackListBC.value
        val newPtnData: Iterator[String] = ptnData.filter(x => !bl.contains(x))
        newPtnData
      })
      newRdd
    }

    // transform的作用把Dstream中的每个RDD拿来做一次操作
    val trueWordsDStream: DStream[String] = wordsDStream.transform(transformFunc)


    val wordsAndOneDStream: DStream[(String, Int)] = trueWordsDStream.map(word => (word, 1))

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

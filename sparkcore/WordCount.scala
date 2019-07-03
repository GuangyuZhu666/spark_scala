package com.spark.test.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    /**
      * step1：获取编程入口
      */
    val config: SparkConf = new SparkConf()
    config.setAppName("wordcount")
    config.setMaster("local")
    val sparkContext: SparkContext = new SparkContext(config)

    /**
      * step2：通过编程入口加载数据
      */
    val linesRDD: RDD[String] = sparkContext.textFile("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkcore/input/word.txt")

    /**
      * step3：对数据进行逻辑处理
      */
    val wordAndCountRDD: RDD[(String, Int)] = linesRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    /**
      * step4：对结果数据进行处理
      */
    wordAndCountRDD.saveAsTextFile("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkcore/output")

    /**
      * step5：关闭编程入口
      */
    sparkContext.stop()


    /**
      * 问题：wordcount这个应用中生成了多少个RDD?
      * 不是4个！
      * 至少4个！
      * 7个 = textFile 2 + flatMap 1 + map 1 + reduceByKey 1 + saveAsTextFile 2
      */
  }

}

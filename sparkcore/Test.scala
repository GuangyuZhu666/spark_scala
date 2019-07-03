package com.spark.test.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit={

    val config = new SparkConf()
    config.setAppName("Test")
    config.setMaster("local")
    val sparkcontext = new SparkContext(config)

    // map
    println("------------------------ map ------------------------")
    val array1 = Array(1, 2, 3, 4, 5, 6)
    val rdd1 : RDD[Int] = sparkcontext.parallelize(array1)
    val rdd2 : RDD[Int] = rdd1.map(x => x+1)
    rdd2.foreach(x => println(x))
    val rdd3 : RDD[(Int, Int)] = rdd1.map(x => (x, x*2))
    rdd3.foreach(x => println(x))


    // flatmap
    println("------------------------ flatmap ------------------------")
    val rdd4 : RDD[String] = sparkcontext.textFile("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparkcore/input/word.txt")
    val rdd5 : RDD[String] = rdd4.flatMap(x => x.split(" "))
    rdd5.foreach(x => println(x))


    // filter
    println("------------------------ filter ------------------------")
    val rdd6 : RDD[Int] = rdd1.filter(x => x%2==0)
    rdd6.foreach(x => println(x))


    // mapPartitions
    println("------------------------ mapPartitions ------------------------")
    val rdd7 : RDD[Int] = sparkcontext.parallelize(array1, 3)
    val rdd8 : RDD[Int] = rdd7.mapPartitions( (x:Iterator[Int]) => {
      println("进行一次处理:")
      val list =  x.toList.map(x => x*x)
      list.toIterator
    })
    rdd8.foreach(x => println(x))


    // mapPartitionsWithIndex
    println("------------------------ mapPartitionsWithIndex ------------------------")
    val rdd9: RDD[Int] = rdd7.mapPartitionsWithIndex((i:Int, x:Iterator[Int]) => {
      println("进行第" + i + "个分区的处理：")
      val list = x.toList.map(x => x*x*x)
      list.toIterator
    })
    rdd9.foreach(x => println(x))


    // sample,takesample
    println("------------------------ sample,takesample ------------------------")
    val rdd10 = rdd1.sample(true, 0.5)
    rdd10.foreach(x => println(x))
    val array2 : Array[Int] = rdd1.takeSample(false, 3)
    array2.foreach(x => println(x))


    // union,cartesian,subtract
    println("------------------------ union,cartesian,subtract ------------------------")
    val array3 = Array(1, 2, 3)
    val array4 = Array(2, 5, 7)
    val rdd11 : RDD[Int] = sparkcontext.parallelize(array3)
    val rdd12 : RDD[Int] = sparkcontext.parallelize(array4)
    val rdd13 : RDD[(Int,Int)] = rdd11.cartesian(rdd12)
    rdd13.foreach(x => println(x))
    val rdd14 : RDD[Int] = rdd11.union(rdd12)
    rdd14.foreach(x => println(x))
    val rdd15 : RDD[Int] = rdd11.subtract(rdd12)
    rdd15.foreach(x => println(x))


    // join
    println("------------------------ join ------------------------")
    val array5 = Array((1,"user1"), (2, "user2"), (2, "user3"))
    val array6 = Array((1,"hello"), (2, "world"), (2, "world1"))
    val rdd16 : RDD[(Int,String)] = sparkcontext.parallelize(array5)
    val rdd17 : RDD[(Int,String)] = sparkcontext.parallelize(array6)
    val rdd18 : RDD[(Int,(String, String))] = rdd16.join(rdd17)
    rdd18.foreach(x => println(x))


    // cogroup
    println("------------------------ cogroup ------------------------")
    val rdd19 : RDD[(Int, (Iterable[String], Iterable[String]))] = rdd16.cogroup(rdd17)
    rdd19.foreach(x => {
      println(x._1 + ", " + x._2._1.mkString("_") + ", " + x._2._2.mkString("_"))
    })


    // reduceByKey
    println("------------------------ reduceByKey ------------------------")
    val list1 = List(("math", 100),("hadoop",98),("spark", 88),("math", 99),("hadoop", 98),("spark", 100))
    val rdd20 : RDD[(String, Int)] = sparkcontext.makeRDD(list1)
    val rdd21 : RDD[(String, Int)]= rdd20.reduceByKey((x,y)=>x+y)
    rdd21.foreach(x =>println(x._1 + "," +x._2))


    // sortByKey
    println("------------------------ sortByKey,sortBy ------------------------")
    val rdd22 : RDD[(Int,String)] = rdd21.map(x => (x._2, x._1))
    val rdd23 : RDD[(Int,String)] = rdd22.sortByKey(false)
    val rdd24 : RDD[(String,Int)] = rdd23.map(x => (x._2, x._1))
    rdd24.foreach(x =>println(x._1 + "," +x._2))
    val rdd25 = rdd24.sortBy(x =>x._2,true)
    rdd25.foreach(x =>println(x._1 + "," +x._2))


    // groupByKey
    println("------------------------ groupByKey ------------------------")
    val rdd26 : RDD[(String, Iterable[Int])] = rdd20.groupByKey()
    rdd26.foreach(x =>println(x._1 + "," +x._2.toList.mkString("_")))
    print("avgScore: ")
    val rdd27 : RDD[(String, Double)]= rdd26.map(x => {
      val avgscore = x._2.sum.toDouble / x._2.count(x=>true).toDouble
      (x._1, avgscore)
    })
    rdd27.foreach(x =>println(x._1 + "," + x._2))


    // aggregateByKey, aggregate
    // aggregate 针对一组单个元素的rdd聚合
    // aggregateByKey = groupByKey + aggregate, 针对key-value,对values聚合
    // zerovalue: 初始值u
    // seqop: u,Int => u
    // combop: u,u => u
    println("------------------------ aggregate,aggregateByKey ------------------------")
    val array7 = Array(1,2,3,4,5,6)
    val rdd28 : RDD[Int] = sparkcontext.parallelize(array7)
    val sumRdd : Int = rdd28.aggregate(0)( (x:Int,y:Int) => x+y,
      (x:Int,y:Int)=>x+y)
    println("sum:"+sumRdd)
    val rdd29 : (Int,Int) = rdd28.aggregate((0,0))((x:(Int,Int),y:Int) => (x._1+y, x._2+1),
      (x:(Int,Int),y:(Int,Int)) => (x._1+y._1, x._2+y._2))
    println("avg = sum: " + rdd29._1 + " / " + rdd29._2 + " = " + rdd29._1.toDouble/rdd29._2.toDouble)
    val rdd30 : RDD[(String,(Int,Int))]= rdd20.aggregateByKey((0,0))((x:(Int,Int),y:Int) => (x._1+y, x._2+1),
      (x:(Int,Int),y:(Int,Int)) => (x._1+y._1, x._2+y._2))
    rdd30.foreach(x=>println(x._1+"'s avgScore = "+x._2._1.toDouble/x._2._2.toDouble))


    // combineByKey
    // 1. createCombiner
    // 这个方法会在每个分区上都执行的，而且只要在分区里碰到在本分区里没有处理过的Key，就会执行该方法。执行的结果就是在本分区里得到指定Key
    // 的聚合类型C（可以是数组，也可以是一个值，具体还是得看方法的定义了。）
    // 2. mergeValue
    // 这方法也会在每个分区上都执行的，和createCombiner不同，它主要是在分区里碰到在本分区内已经处理过的Key才执行该方法，执行的结果就是
    // 将目前碰到的Key的值聚合到已有的聚合类型C中。
    // 其实方法1和2放在一起看，就是一个if判断条件，进来一个Key，就去判断一下若以前没出现过就执行方法1，否则执行方法2.
    // 3. mergeCombiner
    // 前两个方法是实现分区内部的相同Key值的数据合并，而这个方法主要用于分区间的相同Key值的数据合并，形成最终的结果。
    println("------------------------ combineByKey ------------------------")
    val rdd31 : RDD[(String,(Int,Int))] = rdd20.combineByKey(x => (x,1),
      (x:(Int,Int),y:Int) => (x._1+y, x._2+1),
      (x:(Int,Int),y:(Int,Int)) => (x._1+y._1, x._2+y._2))
    rdd31.foreach(x=>println(x._1+"'s avgScore = "+x._2._1.toDouble/x._2._2.toDouble))


    // cashe() ：把结果持久化到"内存"中去,cashe中实际调用persist()
    // persist() : 可以定义持久化级别，仅内存，仅磁盘，内存and磁盘...


    // 广播变量：在传变量时，同一个executor中多个平行化的task共享一个变量，而不是每个task都传
    val a : Int = 1
    rdd1.map(x=>(x,a))
    // 替换成
    val ab  = sparkcontext.broadcast(a)
    rdd1.map(x=>(x,ab.value))

    // 同理，还有累加器


    sparkcontext.stop()

  }


}

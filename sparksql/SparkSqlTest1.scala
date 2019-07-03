package com.spark.test.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

// 笔记：
// dataFrame里元素单位只为row,必须有schema
// schema有两种方式，一种通过实体类将row转换成实体类（schema对应实体类中的属性）
// 一种通过 StrucType[Seq(StructField(colname,type,nullable) ) ]

// dataset中元素可以是具体类，而dataFrame只是row
// 例如dataFrame第一种创建方式虽然有个person类，
// 但是实际上只能看到schema信息而看不到person信息，因为单位只是row

object SparkSqlTest1 {

  def main(args: Array[String]): Unit = {

    val sparkconf : SparkConf  = new SparkConf()
    sparkconf.setAppName("Spark Sql Test1").setMaster("local")
    val sparkContext : SparkContext = new SparkContext(sparkconf)
    val sqlContext : SQLContext = new SQLContext(sparkContext)

    val user = Seq("Zhu", "Guang", "yu")
    val userRdd : RDD[String] = sparkContext.parallelize(user)

    val userRowRdd = userRdd.map(x => Row(x, "6"))
    userRowRdd.cache()

    val userDF1 = sqlContext.createDataFrame(
      userRowRdd.map(x => Person(x(0).toString,x(1).toString)))
    userDF1.show()

    val fieldNames = Seq("name", "id")
    val schemaFields = fieldNames.map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(schemaFields)
    val userDF2 = sqlContext.createDataFrame(userRowRdd, schema)
    userDF2.show()

    // df/ds => Rdd
    userDF2.rdd.collect().foreach(x=>println(x))

    sparkContext.stop()

  }

}

case class Person(name:String,id:String)


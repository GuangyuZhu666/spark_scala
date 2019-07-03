package com.spark.test.sparksql

import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSqlTest {

  def main(args: Array[String]): Unit = {

    val spark : SparkSession = SparkSession
      .builder()
      .appName("Spark Sql Test")
      .master("local")
      .getOrCreate()

    // val studentDF : DataFrame = spark.read.csv("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparksql/input/student.csv")
    // studentDF.show()

    val studentDF : DataFrame = spark.read
      .format("csv")
      .options(Map(("seq",","),("header","true"),("infoschema","true")))
      .load("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparksql/input/student.csv")
    studentDF.cache()
    studentDF.show()

    // DSL
    studentDF.select(("name"),("age")).filter("age<24").show

    // sql
    val stuTempTable  = studentDF.registerTempTable("tb_student")
    spark.sql("select name, age-1 as age from tb_student where age=24 ").show()
    // val stuTempView = studentDF.createGlobalTempView("tv_student")
    // spark.sql("select name, age-1 as age from global_temp.tv_student where age=24 ").show()

    // jdbc
    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=true"
    val tableName = "user"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    val userDF = spark.read.jdbc(url, tableName, properties)
    userDF.write.format("json").save("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparksql/output")

    // write.save默认存储形式parquet，列式:
    // userDF.write.save("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparksql/output")

    // toDF
    val user = Seq("Zhu", "Guang", "yu")
    val userRdd : RDD[String] = spark.sparkContext.parallelize(user)
    val userRowRdd : RDD[Row] = userRdd.map(x => Row(x, "6"))
    import spark.implicits._
    userRowRdd.map(x=>Person(x(0).toString,x(1).toString)).toDF().show()

    //dataset
    val ds = spark.read.json("/Users/guang/Documents/Idea_Project/sparkTest/src/main/scala/com/spark/test/sparksql/input/person.json")
      .as[Person]
    ds.show()

    spark.close()

  }

}



package com.spark.test.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlHive {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkHive")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val stuDF : DataFrame = spark.sql("select * from test.stu")
    stuDF.show()

    // 注册自定义函数
    spark.udf.register("getNameLength", (x:String) => x.length)

    val stuDF2: DataFrame = spark.sql("select name , getNameLength(name) as namelength from test.stu")
    stuDF2.show()

    spark.udf.register("myAvg", MyAvg)
    spark.sql("select myAvg(age) from stu")
    
    spark.close()

    // hive udtf例子http://www.cnblogs.com/longjshz/p/5488748.html
  }

}


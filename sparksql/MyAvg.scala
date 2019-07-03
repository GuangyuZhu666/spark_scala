package com.spark.test.sparksql

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

/**
  * 自定义聚合函数
  */
object MyAvg extends UserDefinedAggregateFunction{

  /**
    * 当前这个函数的输入参数类型
    * MyAvg(age)
    */
  def inputSchema : StructType = StructType(
    StructField("age", DoubleType, false)::Nil
  )

  /**
    * bufferSchema 定义辅助变量
    * 平均年龄 = 年龄和 / 数量
    * avg = total / count
    */
  def bufferSchema : StructType = StructType(
    StructField("total", DoubleType, false) ::
    StructField("count", IntegerType, false) :: Nil
  )

  /**
    * 初始化辅助变量
    * @param buffer 辅助变量，看作map
    * buffer.get(key) = value
    */
  def initialize(buffer : org.apache.spark.sql.expressions.MutableAggregationBuffer) : Unit={
    buffer.update(0, 0)
    buffer.update(1, 0)
  }

  /**
    * 迭代操作, 把input合并到buffer中
    * @param buffer total, count
    * @param input 包含传进来的字段age
    */
  def update(buffer : org.apache.spark.sql.expressions.MutableAggregationBuffer, input : org.apache.spark.sql.Row) : Unit={
    val newTotal = buffer.getDouble(0) + input.getDouble(0)
    val newCount = buffer.getInt(1) + 1
    buffer.update(0, newTotal)
    buffer.update(1, newCount)
  }

  /**
    * 把buffer2合并到buffer1中去
    * @param buffer1
    * @param buffer2
    */
  def merge(buffer1 : org.apache.spark.sql.expressions.MutableAggregationBuffer, buffer2 : org.apache.spark.sql.Row) : Unit={
    buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1))
  }

  /**
    * 最终的计算逻辑，函数的计算结果
    * @param buffer
    * @return
    */
  def evaluate(buffer : org.apache.spark.sql.Row) : scala.Any = buffer.getDouble(0) / buffer.getInt(1)

  // 输出值的类型
  def dataType : DoubleType = DoubleType

  //输入值和输出值一样时，true
  def deterministic : Boolean = true

}

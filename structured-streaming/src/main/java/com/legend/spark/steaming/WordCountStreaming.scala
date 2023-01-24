package com.legend.spark.steaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object WordCountStreaming {

  def main(args: Array[String]): Unit = {

    println("hello world")

    var spark: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val host: String = "localhost"
    val port: String = "9999"

    var df: DataFrame = spark.readStream.format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    /**
     * 使用DataFrame API完成Word Count计算
     */
    // 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
    df = df.withColumn("words", functions.split(col("value"), " "))
      //    df = df.withColumn("words", split($"value", " "))
      // 把数组words展平为单词word
      .withColumn("word", explode(col("words")))
      //      .withColumn("word", explode($"words"))
      // 以单词word为Key做分组
      .groupBy("word")
      // 分组计数
      .count()

    /**
     * 将Word Count结果写入到终端（Console）
     */

    df.writeStream
      // 指定Sink为终端（Console）
      .format("console")

      // 指定输出选项
      .option("truncate", false)

      // 指定输出模式
      .outputMode("complete")
      //.outputMode("update")

      // 启动流处理应用
      .start()
      // 等待中断指令
      .awaitTermination()


  }

}

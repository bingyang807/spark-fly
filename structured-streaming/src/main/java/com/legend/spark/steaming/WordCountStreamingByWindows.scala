package com.legend.spark.steaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, element_at, explode, initcap, split, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * nc -lk 9999
2021-10-01 09:30:00,Apache Spark
2021-10-01 09:34:00,Spark Logo
2021-10-01 09:36:00,Structured Streaming
2021-10-01 09:39:00,Spark Streaming
 */
object WordCountStreamingByWindows {

  def main(args: Array[String]): Unit = {

    println("hello world")
    // 创建sparksession，本地执行
    var spark: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    // 设置需要监听的本机地址与端口号
    // nc -lk 9999
    val host: String = "localhost"
    val port: String = "9999"

    // 数据加载socket数据，创建DataFrame
    /**
     * readStream api
     * SparkSession.readStream.format(Source).option(选项键，选项值).load()
     * Source: socket\file\kafka
     * option：与数据源有关的若关选型
     * load：将数据流加载进 Spark
     */
    var df: DataFrame = spark.readStream.format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    /**
     * 使用DataFrame API完成Word Count计算
     */
    // 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
    // 从 Socket 创建的 DataFrame，默认只有一个“value”列，它以行为粒度，存储着从 Socket 接收到数据流
    df = df.withColumn("input", split(col("value"), ","))
      //    df = df.withColumn("words", split($"value", " "))
      // 提取事件事件
      .withColumn("eventTime", element_at(col("input"), 1).cast("timestamp"))
      .withColumn("words", split(element_at(col("input"), 2), " "))
      // 把数组words展平为单词word
      .withColumn("word", explode(col("words")))
      //      .withColumn("word", explode($"words"))
      // 以tubling window 与 单词word为Key做分组
      .groupBy(window(col("eventTime"), "5 minute"), col("word"))
      // 分组计数
      .count()

    /**
     * 数据输出
     * 支持console\file\kafka\foreach(batch)
     *
     *
     */
    // 将Word Count结果写入到终端（Console）
    df.writeStream
      // 指定Sink为终端（Console）
      .format("console")
      // 指定输出选项
      // Console 相对应的“truncate”选项，用来表明输出内容是否需要截断
      .option("truncate", false)
      // 指定输出模式
      /**
       *  支持三种输出模式：
       *  complete : 输出到目前为止处理过的全部数据
       *  append : 仅输出最近一次作业的计算结果
       *           对于有聚合逻辑的流处理来说，开发者必须要提供 Watermark，才能使用 Append mode
       *  update : 仅输出内容有更新的计算结果
       */
      .outputMode("complete")
      //.outputMode("update")
      // 启动流处理应用
      .start()
      // 等待中断指令
      .awaitTermination()


  }

}

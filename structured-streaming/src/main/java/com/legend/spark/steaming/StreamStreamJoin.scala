package com.legend.spark.steaming

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object StreamStreamJoin {

  def main(args: Array[String]): Unit = {
    // 初始化环境
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    // 数据文件的根路径
    val rootPath: String = "structured-streaming/src/main/resources/data"

    // 初始化流a
    // 定义视频流schema
    val videoSchema = new StructType()
      .add("id", "integer")
      .add("name", "string")
      .add("postTime", "timestamp")
    // 监听视频数据目录，以实时数据流的方式，加载新加入的文件
    val videoStream: DataFrame = spark.readStream
      .format("csv")
      .option("header", true)
      .option("path",s"${rootPath}/videoPosting")
      .schema(videoSchema)
      .load
    // 定义Watermark,设置Late data容忍度
    val videoStreamWithWatermark = videoStream.withWatermark("postTime", "5 minutes")



    // 初始化流b
    // 定义互动流
    val actionSchema = new StructType()
      .add("userId","integer")
      .add("videoId", "integer")
      .add("event","string")
      .add("eventTime","timestamp")
    // 监听interactions目录，以实时数据流的方式，加载新加入的文件
    val actionStream: DataFrame = spark.readStream
      .format("csv")
      .option("header",true)
      .option("path",s"${rootPath}/interactions")
      .schema(actionSchema)
      .load
    // 定义Watermark,设置Late data容忍度
    val actionStreamWithWatermark = actionStream.withWatermark("eventTime", "1 hours")

    // 两个流数据关联
    /**
     * 双流关联，需要缓存并维护状态数据，这主要是为了late data同样能够参与计算，保证计算逻辑的一致性
     * 在 Watermark 机制的“保护”之下，事件时间的限制进一步降低了状态数据需要在内存中保存的时间，从而降低系统资源压力
     *
     */
    val joinDF: DataFrame = actionStreamWithWatermark.join(videoStreamWithWatermark,
      expr(
        """
          videoId = id AND
          eventTime >= postTime AND
          eventTime <= postTime + interval 1 hour
          """))

    // 输出
    joinDF.writeStream
      .format("console")
      .option("truncate", false)
      // 指定输出模式
      // Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode;
      .outputMode("append")
      // 启动流处理
      .start()
      // 等待中断指令
      .awaitTermination()
  }

}

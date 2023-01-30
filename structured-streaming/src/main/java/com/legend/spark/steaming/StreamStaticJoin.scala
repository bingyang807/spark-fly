package com.legend.spark.steaming

import org.apache.spark.sql.functions.{broadcast, col, window}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
+------------------------------------------+------+-------+-----+---+------+---+------+
|window                                    |userId|event  |count|id |name  |age|gender|
+------------------------------------------+------+-------+-----+---+------+---+------+
|{2021-10-01 09:00:00, 2021-10-01 10:00:00}|4     |Comment|1    |4  |David |40 |Male  |
|{2021-10-01 09:00:00, 2021-10-01 10:00:00}|2     |Comment|1    |2  |Bob   |32 |Male  |
|{2021-10-01 09:00:00, 2021-10-01 10:00:00}|3     |Like   |2    |3  |Cassie|18 |Female|
|{2021-10-01 09:00:00, 2021-10-01 10:00:00}|1     |Forward|1    |1  |Alice |26 |Female|
+------------------------------------------+------+-------+-----+---+------+---+------+
 */
object StreamStaticJoin {

  def main(args: Array[String]): Unit = {
    // 保存staging、interactions、userProfile等文件夹的根目录
    val rootPath: String = "structured-streaming/src/main/resources/data"
    // 初始化sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()
    // 使用read API读取离线数据，创建DataFrame
    val staticDF: DataFrame = spark.read
      .format("csv")
      .option("header", true)
      .load(s"${rootPath}/userProfile/userProfile.csv")


    // 定义用户反馈文件的Schema
    val actionSchema = new StructType()
      .add("userId", "integer")
      .add("videoId", "integer")
      .add("event", "string")
      .add("eventTime", "timestamp")

    // 使用readStream API加载数据流
    var streamingDF: DataFrame = spark.readStream
      // 指定文件格式
      .format("csv")
      .option("header", true)
      // 指定监听目录
      .option("path", s"${rootPath}/interactions")
      // 指定数据Schema
      .schema(actionSchema)
      .load

    // 互动数据分组、聚合
    streamingDF = streamingDF
      // 创建Watermark，设置最大容忍度为30分钟
      .withWatermark("eventTime", "30 minutes")
      // 按照事件窗口、userId与互动类型
      .groupBy(window(col("eventTime"), "1 hours"), col("userId"), col("event"))
      // 记录不同事件窗口，用户不同类型互动的计数
      .count()

    /**
     * 流批关联
     * 对于 streamingDF 来说，它所触发的每一个 Micro-batch，都会扫描一次 staticDF 所封装的离线数据
     *
     */
    val jointDF: DataFrame = streamingDF.join(staticDF, streamingDF("userId") === staticDF("id"))

    /**
     * 可以在 staticDF 之上创建广播变量，然后把流批关联原本的 Shuffle Join 转变为 Broadcast Join 来提升执行性能
     */
//    val bc_staticDF = broadcast(staticDF)
//    val joinDF: DataFrame = streamingDF.join(bc_staticDF, streamingDF("userId") === bc_staticDF("id"))

    // 输出
    jointDF.writeStream
      // 指定sink为终端（Console）
      .format("console")
      // 指定输出选项
      .option("truncate", false)
      // 指定输出模式
      .outputMode("update")
      // 启动流处理应用
      .start()
      // 等待中断指令
      .awaitTermination()

  }

}

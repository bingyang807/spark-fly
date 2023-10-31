package com.legend.spark.steaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

/**
 * kafka-topics.sh --bootstrap-server kafka:9092 --create \
 * --topic mem-monitor \
 * --replication-factor 1 \
 * --partitions 1
 *
 *
 * kafka-topics.sh --bootstrap-server kafka:9092 --create \
 * --topic cpu-monitor \
 * --replication-factor 1 \
 * --partitions 1
 *
 *
 * cpu-monitor-agg-result
 * ./bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cpu-monitor-agg-result --from-beginning
 */

object Kafka2KafkAndConsole {

  def main(array: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()


    val dfCPU: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:19092")
      .option("subscribe", "cpu-monitor")
      .load()

//    dfCPU.writeStream
//      .outputMode("append")
////      .outputMode("complete")
//      .format("console")
//    .trigger(Trigger.ProcessingTime(10.seconds))
//    .start()
//    .awaitTermination()

    /**
     * +--------------------+--------------------+-----------+---------+------+--------------------+-------------+
|                 key|               value|      topic|partition|offset|           timestamp|timestampType|
+--------------------+--------------------+-----------+---------+------+--------------------+-------------+
|[61 6C 66 69 65 2...|[31 34 2E 31 36 3...|cpu-monitor|        0|   346|2023-10-31 11:28:...|            0|
|[61 6C 66 69 65 2...|[31 31 2E 31 38 3...|cpu-monitor|        0|   347|2023-10-31 11:28:...|            0|
|[61 6C 66 69 65 2...|[31 35 2E 37 39 3...|cpu-monitor|        0|   348|2023-10-31 11:28:...|            0|
|[61 6C 66 69 65 2...|[31 34 2E 35 32 3...|cpu-monitor|        0|   349|2023-10-31 11:28:...|            0|
|[61 6C 66 69 65 2...|[31 36 2E 32 38 3...|cpu-monitor|        0|   350|2023-10-31 11:28:...|            0|
+--------------------+--------------------+-----------+---------+------+--------------------+-------------+
     */

    import spark.implicits._

    dfCPU
      .withColumn("clientName", $"key".cast(StringType))
      .withColumn("cpuUsage", $"value".cast(StringType))
      .groupBy($"clientName")
      .agg(avg($"cpuUsage").cast(StringType).as("avgCPUUsage"))
      .writeStream
      .outputMode("Complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()
      .awaitTermination()

    /**
     * Batch: 2
-------------------------------------------
+----------+-----------------+
|clientName|      avgCPUUsage|
+----------+-----------------+
|  alfie-id|40.27998455972188|
+----------+-----------------+
     */

    //    dfCPU
    //      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .as[(String, String)]
    dfCPU
      // required column found expression
      .withColumn("key", $"key".cast(StringType))
      .withColumn("value", $"value".cast(StringType))
//      .withColumn("key", col("key").cast(StringType))
//      .withColumn("value", col("value").cast(StringType))
      .groupBy(col("key"))
//      .agg(avg($"value"))
      .agg(avg($"value").cast(StringType).as("value"))
      .writeStream
      .outputMode("Complete")
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:19092")
      .option("topic", "cpu-monitor-agg-result")
      .option("checkpointLocation", "/tmp/checkpoint")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()
      .awaitTermination()

    /**
     * ls /tmp/checkpoint
commits		metadata	offsets		sources		state
     */

    /**
     * ./bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cpu-monitor-agg-result --from-beginning
        48.92621555359135
        49.73407443677476
        45.343124026771086
        51.10823326686715
        48.29981925076419

     */


  }

}

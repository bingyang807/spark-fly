package com.legend.spark

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Dataset, Encoders, Encoder, SparkSession}

object EncoderScalaSample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("EncoderScalaSample")
      .getOrCreate()

    notUseImplicits(spark)
    useSelfDefinedImplicits(spark)
    useSparkImplicits(spark)

    spark.close()
  }
  /**
   *  使用Spark提供的隐式转换(implicits conversion)
   *  调用 Dataset.map[U : Encoder](func: T => U): Dataset[U]
   */
  def useSparkImplicits(spark: SparkSession): Unit = {

    println("=== useSparkImplicits ===")

    val longDataset = spark.range(1, 5)

    // 引用 SQLImplicits中的定义
    // implicit def newStringEncoder: Encoder[String] = Encoders.STRING
    import spark.implicits._
    val stringDataset = longDataset.map(num => s"No: $num")
    stringDataset.printSchema()
    stringDataset.show()
  }

  /**
   *  自己定义 隐式转换(implicits conversion), 这种方式和Java API 相同，
   *  调用 Dataset.map[U : Encoder](func: T => U): Dataset[U]
   */
  def useSelfDefinedImplicits(spark: SparkSession): Unit = {

    println("=== useSelfDefinedImplicits ===")

    val longDataset = spark.range(1, 5)

    // 定义 implicit parameter： newStringEncoder
    implicit def newStringEncoder: Encoder[String] = Encoders.STRING
    val stringDataset = longDataset.map(num => s"No: $num")
    //val stringDataset = longDataset.map(num => s"No: $num")(Encoders.STRING)
    stringDataset.printSchema()
    stringDataset.show()

    // 定义 implicit parameter： numberObjectEncoder
    implicit def numberObjectEncoder: Encoder[NumberObject] = Encoders.bean(classOf[NumberObject])
    val numberObjectDataset = longDataset.map(num => new NumberObject(num))
    numberObjectDataset.printSchema()
    numberObjectDataset.show()
  }

  /**
   *  不用 scala 的 隐式转换(implicits conversion), 这种方式和Java API 相同，
   *  调用 Dataset.map[U](func: MapFunction[T, U], encoder: Encoder[U]): Dataset[U]
   */
  def notUseImplicits(spark: SparkSession): Unit = {

    println("=== notUseImplicits ===")

    val longDataset = spark.range(1, 5)

    val stringDataset: Dataset[String] = longDataset.map(
      new MapFunction[java.lang.Long, String] {
        override def call(t: java.lang.Long): String = {
          s"No: $t.toString"
        }
      }.asInstanceOf[MapFunction[java.lang.Long, String]],
      Encoders.STRING
    )
    stringDataset.printSchema()
    stringDataset.show()

    val numberObjectDataset: Dataset[NumberObject] = longDataset.map(
      new MapFunction[java.lang.Long, NumberObject]() {
        override def call(t: java.lang.Long): NumberObject = {
          new NumberObject(t)
        }
      }.asInstanceOf[MapFunction[java.lang.Long, NumberObject]], Encoders.bean(classOf[NumberObject]))
    numberObjectDataset.printSchema()
    numberObjectDataset.show()
  }
}

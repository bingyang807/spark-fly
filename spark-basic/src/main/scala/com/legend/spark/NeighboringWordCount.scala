package com.legend.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计相邻单词共现的次数,取前5名
 */
object NeighboringWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NeighboringWordCount").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val rootPath: String = "file://"
    val file: String = s"${rootPath}/Users/alfie/workspace/code/learn/spark-fly/spark-basic/src/main/resources/wikiOfSpark.txt"

    // 读取文件内容
    val lineRDD: RDD[String] = sparkContext.textFile(file)

    // 分词
    val wordPairRDD: RDD[String] = lineRDD.flatMap(line => {
      val words: Array[String] = line.split(" ")
        .filter(word => !word.equals(""))
      for(i <- 0 until words.length - 1 ) yield words(i) + "-" + words(i + 1)
    })

    // 分组计数
    val kvRDD: RDD[(String, Int)] = wordPairRDD.map(wordPair => (wordPair, 1))

    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

    // 根据计数值排序，获取最多的前5个
    wordCounts.map{case (k, v) => (v, k)}
      // 降序
      .sortByKey(false)
      // action ： take 数据收集
      .take(5)
      // 打印
      .foreach(println)
  }

}

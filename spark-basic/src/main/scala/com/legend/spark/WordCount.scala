package com.legend.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *  Word Count 的初衷是对文件中的单词做统计计数，打印出频次最高的 5 个词汇
 *  包含如下三个步骤：
 *  1. 读取内容： 调用Spark文件读取API ，加载wikiOfSpark.txt
 *  2. 分词： 以行为单位，把句子打散为单词
 *  3. 分组计数： 按照单词分组计数
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    // create sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      // .master("local[3]")
      .getOrCreate()
    val sc = spark.sparkContext
    // 这里的下划线"_"是占位符，代表数据文件的根目录
    val rootPath: String = "/Users/alfie/workspace/code/learn/spark-fly/spark-basic/src/main/resources"
//     val rootPath: String = _
    val file: String = s"${rootPath}/wikiOfSpark.txt"

    // 读取文件内容
    val lineRDD: RDD[String] = sc.textFile(file)
    println("initial partition count:" + lineRDD.getNumPartitions)

    val reparRdd = lineRDD.repartition(4)
    println("re-partition count:" + lineRDD.getNumPartitions)

    // 以行为单位分词
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
    wordRDD.foreach(f => println(f))
    // 过滤空字符串
    val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
    cleanWordRDD.foreach(f => println(f))

    //把RDD元素String，转换为（key, value）的形式
    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
    kvRDD.foreach(println)
    //按照单词做分组计数
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey(_ + _)
//    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
    wordCounts.foreach(println)

    // 打印词频最高的5个词汇
    // map{case (k, v) => (v, k)}，这一步是把（Key，Value）对调，目的是按照“计数”来排序。
    // 比如，原来是（Spark，63），这一步之后，这条记录就变成了（63，Spark），不妨把这一步拆解开来，用first探索一下
    // sortByKey(false) 是降序排序，注意，这时候的Key，不再是单词了，而是调换顺序之后的Value（比如63），也就是单词计数
    val top5Word = wordCounts.map { case (k, v) => (v, k) }.sortByKey(false).take(5)
    top5Word.foreach(println)

//    wordCounts.saveAsTextFile("/Users/alfie/workspace/code/learn/spark-fly/spark-basic/src/main/resources/wordcount.txt")

  }

}

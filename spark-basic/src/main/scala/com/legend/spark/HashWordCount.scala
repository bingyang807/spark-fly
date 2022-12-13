package com.legend.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.security.MessageDigest

/**
 * Word Count 的初衷是对文件中的单词哈希值做统计计数，打印出频次最高的 5 个词汇
 * 包含如下三个步骤：
 *  1. 读取内容： 调用Spark文件读取API ，加载wikiOfSpark.txt
 *     2. 分词： 以行为单位，把句子打散为单词，并取hash值
 *     3. 分组计数： 按照单词分组计数
 */
object HashWordCount {

  // 定义映射函数f
  def f(word: String): (String, Int) = {
    if (word.equals("Spark")) return (word, 2)
    return (word, 1)
  }

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

    //把RDD元素String，按照自定义函数f，转换为（key, value）的形式
    //    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
    //    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(f)
    // 每个单词的hash值作为pair的key值
    // 但这种每个元素都需要生成MD5对象，但数据量上百万甚至上亿级别，那么实例话对象的开销就会聚沙成塔
    //    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map{ word =>
    //      // 获取MD5对象实例
    //      val md5 = MessageDigest.getInstance("MD5")
    //      // 使用MD5计算哈希值
    //      val hash = md5.digest(word.getBytes).mkString
    //      // 返回哈希值与数字1的pair
    //      (hash, 1)
    //    }
    // 可以考虑mapPartitions,共性部分，如生成MD5实例、连接远端数据库connections, 文件句柄、在线推理的机器学习模型
    // 对于一个有着上百万条记录的 RDD 来说，其数据分区的划分往往是在百这个量级，因此，相比 map 算子，mapPartitions 可以显著降低对象实例化的计算开销，这对于 Spark 作业端到端的执行性能来说，无疑是非常友好的
    val kvRDD: RDD[(String, Int)] = cleanWordRDD.mapPartitions(partition => {
      // 这里以数据分区为粒度，获取MD5对象
      val md5 = MessageDigest.getInstance("MD5")
      val newPartition = partition.map(word => {
        // 在处理每一条数据记录的时候，可以复用同一个partition内的MD5对象
        (md5.digest(word.getBytes()).mkString, 1)
    })
      newPartition})
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

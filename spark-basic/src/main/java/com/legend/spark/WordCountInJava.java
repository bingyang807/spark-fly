package com.legend.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountInJava {
    public static void main(String[] args){
        // 初始spark环境
//        final SparkSession sparkSession = SparkSession.builder()
//                .master("local[2]")
//                .appName("Word Count")
//                .getOrCreate();
        SparkConf sparkConf = new SparkConf()
                .setAppName("Word Count")
                .setMaster("local[*]");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

//        final SparkContext sparkContext = sparkSession.sparkContext();

        // 读取文件内容
        // 注意分区数不同，输出结果不同，会按照分区排序，无法做到全局排序
        final JavaRDD<String> lineRDD = javaSparkContext.textFile("/Users/alfie/workspace/code/learn/learn-spark/spark-fly/spark-basic/src/main/resources/wikiOfSpark.txt", 3);
//        final JavaRDD<String> lineRDD = javaSparkContext.textFile("/Users/alfie/workspace/code/learn/learn-spark/spark-fly/spark-basic/src/main/resources/wikiOfSpark.txt", 1);

        // 以行为单位做分词
        final JavaRDD<String> wordRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\\s+")).iterator();
            }
        });
        final JavaRDD<String> cleanWordRDD = wordRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return !v1.equals("");
            }
        });
        // 把单词转换为键值对
        final JavaPairRDD<String, Integer> kvRDD = cleanWordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        // 键值对做分组计数
        final JavaPairRDD<String, Integer> wordCounts = kvRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 交换位置排序，取top5
        wordCounts.mapToPair(row -> new Tuple2<>(row._2, row._1)).sortByKey(false)
                .foreach(new VoidFunction<Tuple2<Integer, String>>(){

                    @Override
                    public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        System.out.println(integerStringTuple2._1 + ":" + integerStringTuple2._2);
                    }
                });
        // 清理
        javaSparkContext.close();
    }
}

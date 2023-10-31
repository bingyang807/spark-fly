package com.legend.spark;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class EncoderJavaSample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .appName("EncoderJavaSample")
        .master("local[1]")
        .getOrCreate();

    Dataset<Long> longDataset = spark.range(1, 5);

    /*
      Java API 调用 Dataset.map[U](func: MapFunction[T, U], encoder: Encoder[U])
     */
    Dataset<String> stringDataset = longDataset
        .map((MapFunction<Long, String>) num -> String.format("No: %s", num), Encoders.STRING());
    stringDataset.printSchema();
    stringDataset.show();

    Encoder<NumberObject> numberObjectEncoder = Encoders.bean(NumberObject.class);
    Dataset<NumberObject> numberObjectDataset = longDataset
        .map((MapFunction<Long, NumberObject>) NumberObject::new, numberObjectEncoder);
    numberObjectDataset.printSchema();
    numberObjectDataset.show();

    numberObjectDataset.foreach((ForeachFunction<NumberObject>) obj -> System.out.println(obj));

    // lambda的等价
    /*
    numberObjectDataset.foreach(new ForeachFunction<NumberObject>() {
      @Override
      public void call(NumberObject numberObject) throws Exception {
        System.out.println(numberObject);
      }
    });
    */

    spark.close();

  }
}

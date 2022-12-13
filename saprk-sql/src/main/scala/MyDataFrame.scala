import org.apache.spark.{SPARK_BRANCH, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 *
 *  spark支持多重数据源：
 *  1. driver端自定义数据结构： 数组、列表、映射
 *    createDataFrame 方法有两个形参，第一个参数正是 RDD，第二个参数是 Schema。createDataFrame 要求 RDD 的类型必须是 RDD[Row]
 *  2. 文件系统：本地文件系统、hdfs、s3
 *    无论哪种文件系统，spark都通过sparkSession的read api 来读取数据并创建DataFrame
 *    sparkSession.read.format(文件格式).option(选项键，选项值).load(文件路径)
 *    第一个类参数文件格式，就是文件的存储格式，如csv(comma separated values)、text、parquet、orc、json、zip、image
 *    CSV 文件格式可以通过 option(“header”, true)，来表明 CSV 文件的首行为 Data Schema，但其他文件格式就没有这个选型
 *    加载选项可以有零个或是多个，当需要指定多个选项时，我们可以用“option(选项 1, 值 1).option(选项 2, 值 2)”的方式来实现
 *    第 3 类参数是文件路径，这个参数很好理解，它就是文件系统上的文件定位符。
 *    比如本地文件系统中的“/dataSources/wikiOfSpark.txt”，HDFS 分布式文件系统中的“hdfs://hostname:port/myFiles/userProfiles.csv”，或是 Amazon S3 上的“s3://myBucket/myProject/myFiles/results.parquet”
 *    https://docs.databricks.com/external-data/index.html
 *  3. RDBMS: mysql、oracle
 *  4. 数据仓库： hive、snowflake
 *  5. nosql： mongodb、hbase、redis
 *  6. 其它计算引擎: kafka、cassandra、elasticsearch
 */

object MyDataFrame {

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    // 1. driver创建dataframe
    val seq: Seq[(String, Int)] = Seq(("Bob", 14), ("Alice", 18))
    val rdd: RDD[(String, Int)] = sc.parallelize(seq)

    // 有了 RDD 之后，再加上Schema
    // StructType用于定义并封装Schema, StructField用于定义Schema中的每一个字段
    // 包含字段名、字段类型，而像StringType、IntegerType这些*Type类型，表示的正是字段类型
    val schema: StructType = StructType(Array(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    // createDataFrame 方法有两个形参，第一个参数正是 RDD，第二个参数是 Schema。createDataFrame 要求 RDD 的类型必须是 RDD[Row]
    val rowRDD: RDD[Row] = rdd.map(fields => Row(fields._1, fields._2))
    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)
    dataFrame.show

    // 可以直接在 RDD 之后调用 toDF 方法来直接转换rdd为dataframe
    // 显示导入了 spark.implicits 包中的所有方法，然后通过在 RDD 之上调用 toDF 就能轻松创建 DataFrame
    // 关键在于 spark.implicits 这个包提供了各种隐式方法
    // 隐式方法是 Scala 语言中一类特殊的函数，这类函数不需要开发者显示调用，函数体中的计算逻辑在适当的时候会自动触发。
    // 正是它们在背后默默地帮我们用 seq 创建出 RDD，再用 createDataFrame 方法把 RDD 转化为 DataFrame
//    import spark.implicits._
//    val dataFrame2: DataFrame = rdd.toDF
//    dataFrame2.printSchema

    // 2. 从csv创建dataframe
    // 加载选项  可选值      默认值  含义
    // header true,false    false  csv文件的第一行，是否为data schema；否则使用“_c”加序号的方式来命名每一个数据列，比如“_c0”、“_c1”，等等
    // seq    任意字符       逗号      数据列之间的分隔符
    // escape 任意字符       反斜线\    转义字符
    // nullValue 任意字符    ""        声明文件中的空值
    // dataFormat simpledateformat字符串  yyyy-MM-dd 声明日期格式
    // mode      permissive,dropMalformed,failFast permissive 读取模式
    // permissive: 容忍度最高，遇到脏数据，将脏数据替换为null，正常加载数据并创建dataframe
    // dropmalformed: 有的商量：遇到脏数据则跳过加载，不让他进入待创建的dataframe,其它数据则正常加载
    // failfast: 任性：遇到脏数据立即报错并退出，创建dataframe失败
    val csvFilePath: String = "/Users/alfie/workspace/code/learn/spark-fly/saprk-sql/src/main/resources/data1.csv"
    val df: DataFrame = spark.read.format("csv").option("header", true).load(csvFilePath)
    df.show
    // 要想加载过程中，为dataframe的每一列指定数据类型，需要显示定义dataschema，并在read api中调用schema方法
    val df2: DataFrame = spark.read.format("csv").schema(schema).option("header", true).load(csvFilePath)
    df2.show
    // 使用read api 读取Parquet文件
    val parquetFilePath: String = "/Users/alfie/workspace/code/learn/spark-fly/saprk-sql/src/main/resources/CarLottery-2011-2019/apply/batchNum=201706/part-00001-f8bb8e7b-904f-42a7-a616-a413406f06fb.c000.snappy.parquet"
    val parquetDF: DataFrame = spark.read.format("parquet").load(parquetFilePath)
    parquetDF.show

    // read api 读取orc文件
//    val orcFilePath: String = _
//    val orcDF: DataFrame = spark.read.format("orc").load(orcFilePath)

    // 3. 从RDBMS创建dataFrame
    val sqlQuery: String = "select * from db_example.user "
    // java.sql.SQLSyntaxErrorException: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'select * from db_example.user WHERE 1=0' at line 1
    val dbDataFrame: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("user", "root")
      .option("password", "root")
      .option("numPartitions", 20)
//      .option("dbtable", sqlQuery)
      .option("dbtable", "db_example.user")
      .load()
    dbDataFrame.show

  }


}

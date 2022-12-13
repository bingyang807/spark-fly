import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 通过 3 种途径来实现 Spark with Hive 的集成方式，它们分别是：
 * 1.创建 SparkSession，访问本地或远程的 Hive Metastore；
 *     1) 首先启动hive metastore hive --service metastore
 *     2) config指定hive.metastore.uris参数
 *      或者spark读取ive配置文件hive-site.xml(可拷贝到spark conf子目录，spark会自行读取其中的配置内容)
 * 2. 通过 Spark 内置的 spark-sql CLI，访问本地 Hive Metastore；
 * 3.通过 Beeline 客户端，访问 Spark Thrift Server。
 */
object SparkWithHive {
  def main(args: Array[String]): Unit = {
    // 假设 Hive 中有一张名为“salaries”的薪资表，每条数据都包含 id 和 salary 两个字段，表数据存储在 HDFS

    // 创建SparkSession实例
    val spark = SparkSession.builder()
        .config("hive.metastore.uris", s"thrift://hiveHost:9083")
        .enableHiveSupport()
        .getOrCreate()
    // 读取Hive表，创建DataFrame
    // 利用 createTempView 函数从数据文件创建临时表的方法，临时表创建好之后，
    // 就可以使用 SparkSession 的 sql API 来提交 SQL 查询语句。
    // 连接到 Hive Metastore 之后，就可以绕过第一步，直接使用 sql API 去访问 Hive 中现有的表，
    // createTempView 函数创建的临时表，它的生命周期仅限于 Spark 作业内部，
    // 这意味着一旦作业执行完毕，临时表也就不复存在，没有办法被其他应用复用。
    // Hive 表则不同，它们的元信息已经持久化到 Hive Metastore 中，
    // 不同的作业、应用、甚至是计算引擎，如 Spark、Presto、Impala，等等，
    // 都可以通过 Hive Metastore 来访问 Hive 表。
    // 在这种集成方式中，Spark 仅仅是“白嫖”了 Hive 的 Metastore，拿到数据集的元信息之后，Spark SQL 自行加载数据、自行处理
    val df: DataFrame = spark.sql("select * from salaries")
    df.show
  }
}

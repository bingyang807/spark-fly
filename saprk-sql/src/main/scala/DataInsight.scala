import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *  数据探察
 *  北京市小汽车摇号
 *  准备了 2011 年到 2019 年北京市小汽车的摇号数据
 *  根目录下有 apply 和 lucky 两个子目录，apply 目录的内容是 2011-2019 年各个批次参与摇号的申请号码，
 *  而 lucky 目录包含的是各个批次中签的申请号码
 *  参与过摇号的人叫“申请者”，把中签的人叫“中签者”。apply 和 lucky 的下一级子目录是各个摇号批次，
 *  而摇号批次目录下包含的是 Parquet 格式的数据文件
 */
object DataInsight {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setExecutorEnv("spark.executor.memory","4g")

    // create sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()


    val rootPath: String = "/Users/alfie/workspace/data/CarLottery-2011-2019"
    // 申请者数据
    val path_apply: String = s"${rootPath}/apply"
    val applyNumbersDF: DataFrame = spark.read.parquet(path_apply)
    // 数据打印
//    applyNumbersDF.show

    // 中签者数据
    val path_lucky: String = s"${rootPath}/lucky"
    // 通过read api读取源文件
    val luckyDogsDF: DataFrame = spark.read.parquet(path_lucky)
    // 数据打印
    luckyDogsDF.show
  }
}

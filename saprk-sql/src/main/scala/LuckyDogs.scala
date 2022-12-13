import org.apache.spark.sql.functions.{col, count, lit, max}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 *  业务需求实现：
 *  研究倍率与中签率之间，到底有没有关系
 *  对于倍率这个数值，官方的实现略显粗暴，如果去观察 apply 目录下 2016 年以后各个批次的文件，你就会发现，所谓的倍率，实际上就是申请号码的副本数量
 *  比如说，我的倍率是 8，那么在各个批次的摇号文件中，我的申请号码就会出现 8 次
 */
object LuckyDogs {

  def main(args: Array[String]): Unit = {
    // create sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
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
//    luckyDogsDF.show

    // 过滤2016年(该年推出倍率政策)以后的中签数据，且仅抽取中签号码carNum字段
    val filteredLuckyDogs: DataFrame = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")

    // 摇号数据与中签数据做内关联，Join key为中签号码carNum
    val jointDF: DataFrame = applyNumbersDF.join(filteredLuckyDogs, Seq("carNum"), "inner")


    // 要统计某个申请号码的倍率，我们只需要统计它在批次文件中出现的次数就可以达到目的
    // 同一个申请号码，在不同批次中的倍率是不一样的
    // 以batchNum、carNum做分组，统计倍率系数
    val multipliers: DataFrame = jointDF.groupBy(col("batchNum"), col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))

    // 要研究的是倍率与中签率的关系，所以只需要关心中签者是在多大的倍率下中签的就行。因此，对于同一个申请号码，我们只需要保留其中最大的倍率就可以了
    // 取最大倍率的做法，会把倍率的统计基数变小，从而引入幸存者偏差。更严谨的做法，应该把中签者过往的倍率也都统计在内，这样倍率的基数才是准确的。不过呢，结合实验，幸存者偏差并不影响“倍率与中签率是否有直接关系”这一结论。因此，咱们不妨采用取最大倍率这种更加简便的做法
    // 以carNum做分组，保留最大的倍率系数
    val uniqueMultipliers: DataFrame = multipliers.groupBy("carNum")
      .agg(max("multiplier").alias("multiplier"))

    // 我们想知道的是，不同倍率之下的人数分布是什么样子的。换句话说，这一次，我们要按照倍率来对数据做分组，然后计算不同倍率下的统计计数
    // 以multiplier倍率做分组，统计人数
    val result: DataFrame = uniqueMultipliers.groupBy("multiplier")
      .agg(count(lit(1)).alias("cnt"))
      .orderBy("multiplier")

    result.collect
    result.show()
    
  }
}

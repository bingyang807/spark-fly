import org.apache.spark.sql.functions.{asc, avg, desc, explode, hash, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 *  为了给开发者提供足够的灵活性，对于 DataFrame 之上的数据处理，
 *  Spark SQL 支持两类开发入口：一个是大家所熟知的结构化查询语言：SQL，另一类是 DataFrame 开发算子。
 *  就开发效率与执行效率来说，二者并无优劣之分，选择哪种开发入口，完全取决于开发者的个人偏好与开发习惯
 *  对于任意的 DataFrame，我们都可以使用 createTempView 或是 createGlobalTempView 在 Spark SQL 中创建临时数据表
 *  两者的区别在于，createTempView 创建的临时表，其生命周期仅限于 SparkSession 内部，
 *  而 createGlobalTempView 创建的临时表，可以在同一个应用程序中跨 SparkSession 提供访问
 *
 *  Spark SQL 还提供大量 Built-in Functions（内置函数），用于辅助数据处理，如 array_distinct、collect_list，等等。
 *  可以浏览官网的Built-in Functions 页面查找完整的函数列表:https://spark.apache.org/docs/3.0.1/api/sql/index.html。
 *  结合 SQL 语句以及这些灵活的内置函数，以应对数据探索、数据分析这些典型的数据应用场景
 *
 */
object MyDataProcess {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    // 1. 利用spark sql 创建临时表，通过sql来数据探察和分析

    val seq = Seq(("Alice", 18), ("Bob", 14))
    //使用 spark.implicits._ 隐式方法通过 toDF 来创建 DataFrame，然后在其上调用 createTempView 来创建临时表“t1”
    import spark.implicits._
    val df = seq.toDF("name", "age")

    df.createTempView("t1")

    val query: String = "select * from t1"

    val result: DataFrame = spark.sql(query)
    // 与 RDD 的开发模式一样，DataFrame 之间的转换也属于延迟计算，当且仅当出现 Action 类算子时
    // ，如上表中的 show，所有之前的转换过程才会交付执行
    result.show

    // 2. 用dataframe做数据处理
    // 2.1 rdd同源类算子：
    //    数据转换 map、mapPartitions flatmap filter
    //    数据聚合 groupByKey reduce
    //    数据准备 union sample
    //    数据预处理 repartition coalesce
    //    数据收集 first take collect
    // 2.2 探索类算子
    //    查看数据模式 columns schema printSchema
    //    查看数据的执行计划 explain
    //    查看数据分布 describe
    //    查看数据模样 show
    val employees = Seq((1, "John", 26, "Male"), (2, "Lily", 28, "Female") , (3, "Raymond", 30, "Male"))
    val employeesDF: DataFrame = employees.toDF("id", "name", "age", "gender")

    employeesDF.printSchema
    /*
    root
    |-- id: integer (nullable = false)
    |-- name: string (nullable = true)
    |-- age: integer (nullable = false)
    |-- gender: string (nullable = true)
    */

    // 2.3 清洗类算子
    //   drop 删除dataframe的列数据
    //        对于上述的 employeesDF，假设我们想把性别列清除，那么直接调用 employeesDF.drop(“gender”)
    //   dropDuplicates 按照指定列去重
    //   na null值处理
    //   distince 去重
    employeesDF.dropDuplicates("gender").show
    // 2.4 转换类算子
    //   select 按照列名对数据做投影 select 算子在功能方面不够灵活
    //   selectExpr 以sql语句为参数生成、提取数据 在灵活性这方面，selectExpr 做得更好
    //   withColumn 生成新的数据列
    //   withColumnRenamed 字段重命名
    //   where 以sql语句为参数做数据过滤
    //   explode 展开数组类的数据列
    employeesDF.selectExpr("id", "name", "concat(id, '_', name) as id_name").show
    // 如果打算把 employeesDF 当中的“gender”重命名为“sex”，就可以用 withColumnRenamed 来帮忙：
    employeesDF.withColumnRenamed("gender", "sex").show
    // withColumnRenamed 是重命名现有的数据列，而 withColumn 则用于生成新的数据列
    // 基于年龄列，我们想生成一列脱敏数据，隐去真实年龄
    // 有了新的数据列之后，我们就可以调用刚刚讲的 drop，把原始的 age 字段丢弃掉。
    employeesDF.withColumn("crypto", hash($"age")).show
    val seq2 = Seq( (1, "John", 26, "Male", Seq("Sports", "News")),(2, "Lily", 28, "Female", Seq("Shopping", "Reading")),(3, "Raymond", 30, "Male", Seq("Sports", "Reading")))
    val employeesDF2: DataFrame = seq2.toDF("id", "name", "age", "gender", "interests")
    employeesDF2.show
    //想把数组元素展开，让每个兴趣都可以独占一条数据记录。
    //这个时候就可以使用 explode，再结合 withColumn，生成一列新的 interest 数据。
    //这列数据的类型是单个元素的 String，而不再是数组。有了新的 interest 数据列之后，我们可以再次利用 drop 算子，把原本的 interests 列抛弃掉
    employeesDF2.withColumn("interest", explode($"interests"))

    // 2.5 分析类算子
    // 前面的探索、清洗、转换，都是在为数据分析做准备。在大多数的数据应用中，数据分析往往是最为关键的那环，甚至是应用本身的核心目的
    //   join 两个dataframe之间做数据关联
    //   groupBy 按照某些列对数据做分组
    //   agg 分组后做数据聚合
    //   sort orderBy 按照某些列做排序
    // 准备两张数据表：employees 和 salaries，也即员工信息表和薪水表，
    // 通过对两张表做数据关联，来分析员工薪水的分布情况
    val seq3 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
    val salaries: DataFrame = seq3.toDF("id", "salary")
    // join算子有三个：第一个待关联的数据表，第二个关联键，两张表之间依据哪些字段做关联，
    // 第三个关联形式：inner\left\right\semi\anti
    val jointDF: DataFrame = salaries.join(employeesDF, Seq("id"), "inner")
    // 以性别为维度，统计不同性别下的总薪水和平均薪水，借此分析薪水与性别之间可能存在的关联关系
    jointDF.show
    val aggResult:DataFrame = jointDF.groupBy("gender").agg(sum("salary").as("sum_salary"), avg("salary").as("avg_salary"))
    aggResult.show
    aggResult.sort(desc("sum_salary"), asc("gender")).show
    // 2.6 持久化算子
    //   write
    // dataFrame.write.format(文件格式).option(选项键，选项值).save(文件路径)
    // read api中，mode选项键用于指定读取模式，如permissive, dropMalformed,failFast
    // write api中，mode用于指定"写入模式"，分别有：
    // append： 以追加的方式写入数据
    // overwrite： 以覆盖的方式写入数据
    // errorifexists 如果目标存储路径已存在，则报异常
    // ignore ：如果目标存储入境已存在，则放弃数据写入

  }

}

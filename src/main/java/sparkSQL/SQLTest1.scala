package sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection.Schema
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SQLTest1
 * @author: Cypress_Xiao
 * @description: sparkSQL入门案例
 * @date: 2022/7/11 11:10
 * @version: 1.0
 */
object SQLTest1 {
  def main(args:Array[String]):Unit = {

    val spark:SparkSession = SparkSession.builder()
      .appName("sql入门示例")
      .master("local[*]")
      .getOrCreate()

    /*
    根据数据自定义结构
     */
    val struct:StructType = new StructType()
      .add("uid", DataTypes.IntegerType)
      .add("name", DataTypes.StringType)
      .add("sub", DataTypes.StringType)
      .add("score", DataTypes.DoubleType)



    //val sc:SparkContext = spark.sparkContext //进入RDD入口
    val dataFrame:DataFrame = spark.read.schema(struct).csv("data/score.csv")
    //将描述数据源的数据和结构的dataFrame对象实例注册成表(视图)
    dataFrame.createTempView("tb_score")


   /* //打印结构
    dataFrame.printSchema()
    //展示数据
    dataFrame.show()*/

    //用sql语句处理视图中的数据
    spark.sql(
      """
        |select
        |name,
        |round(avg(score),2) as avg_score
        |from
        |tb_score
        |group by
        |name
        |""".stripMargin
    ).show()

    spark.close()
  }

}

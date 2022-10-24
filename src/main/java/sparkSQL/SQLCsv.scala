package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SQLCsv
 * @author: Cypress_Xiao
 * @description: 读取csv文件数据
 * @date: 2022/7/11 14:41
 * @version: 1.0
 */
object SQLCsv {
  def main(args:Array[String]):Unit = {
    /**
     * 加载无头csv文件
     */
    val spark:SparkSession = SparkUtil.getSession("解析csv文件")
    val df1:DataFrame = spark.read.csv("file:///D:\\AllContent\\spark\\data\\score.csv")
    df1.printSchema()
    println("-------------------------------------------------------------")

    /**
     * 加载有头的csv文件 ("header",true)
     */
    val df2:DataFrame = spark.read.option("header", value = true).csv("file:///D:\\AllContent\\spark\\data\\score1.csv")
    df2.printSchema()
    println("-------------------------------------------------------------")

    /**
     * 自动类型推导 ("inferSchema",true)
     */
    val df3:DataFrame = spark.read.option("inferSchema", value = true)
      .option("header", value = true).csv("file:///D:\\AllContent\\spark\\data\\score1.csv")
    df3.printSchema()
    println("-------------------------------------------------------------")

    /**
     * 自定义数据类型和字段名字
     */
    val struct:StructType = new StructType()
      .add("uid", DataTypes.LongType)
      .add("name", DataTypes.StringType)
      .add("sub", DataTypes.StringType)
      .add("score", DataTypes.DoubleType)

    val df4:DataFrame = spark.read.schema(struct).option("header", value = true).csv("file:///D:\\AllContent\\spark\\data\\score1.csv")
    df4.printSchema()
    println(df4.rdd.partitions.length)
    df4.write.parquet("data/score.parquet")
    println("-------------------------------------------------------------")
  }

}

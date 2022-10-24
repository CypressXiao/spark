package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SparkParquet
 * @author: Cypress_Xiao
 * @description: 加载orc文件
 * @date: 2022/7/11 15:24
 * @version: 1.0
 */
object SparkParquet {
  def main(args:Array[String]):Unit = {
    val spark:SparkSession = SparkUtil.getSession("加载Orc文件")
    val df1:DataFrame = spark.read.parquet("data/score.parquet")
    df1.printSchema()
    df1.show()

  }

}

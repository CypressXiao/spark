package sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SparkHive
 * @author: Cypress_Xiao
 * @description: 加载hive中的数据
 * @date: 2022/7/11 16:01
 * @version: 1.0
 */
object SparkHive {
  def main(args:Array[String]):Unit = {
    val spark:SparkSession = SparkSession.builder().appName("spark加载hive").master("local[*]")
      .enableHiveSupport().getOrCreate()

    val df:DataFrame = spark.sql("select * from doit32.users")

    df.printSchema()
    df.show()
  }

}

package sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: SQLTest2
 * @author: Cypress_Xiao
 * @description: spark解析 json
 * @date: 2022/7/11 11:57
 * @version: 1.0
 */
object SQLTest2 {
  def main(args:Array[String]):Unit = {

    val spark:SparkSession= SparkSession.builder().appName("json解析")
      .master("local[*]").getOrCreate()

    val frame:DataFrame = spark.read.json("data/stu.json")

    frame.select("sid").where("sid<3").show()

    frame.createTempView("tb_stu")

    spark.sql(
      """
        |select
        |name,
        |avg(score)
        |from
        |tb_stu
        |group by name
        |"""
        .stripMargin).show()

    spark.close()
  }

}

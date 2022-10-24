package sparkSQL

import java.util.Properties

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark
 * @package: sparkSQL
 * @objectName: SparkMySql
 * @author: Cypress_Xiao
 * @description: 加载mysql中数据
 * @date: 2022/7/11 15:53
 * @version: 1.0
 */
object SparkMySql {
  def main(args:Array[String]):Unit = {
    val spark:SparkSession = SparkUtil.getSession("加载mysql中数据 ")
    //创建配置文件信息对象
    val pro = new Properties()
    //配置用户名及密码
    pro.setProperty("user","root")
    pro.setProperty("password","123456")

    //读取jdbc数据路径
    val df:DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/my_db1", "location_info", pro)
    df.printSchema()
    df.show()

    df.createTempView("geo")

    spark.sql(
      """
        |select
        |province,
        |count(province) as num
        |from
        |geo
        |group by province
        |""".stripMargin).show()
    //spark.close()


  }

}

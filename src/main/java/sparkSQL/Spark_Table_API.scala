package sparkSQL

import SparkCore.utils.SparkUtil
import org.apache.spark.sql.SparkSession

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: Spark_Table_API
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/13 15:03
 * @version: 1.0
 */
object Spark_Table_API {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkSession.builder()
      .appName("Spark-Table-API")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()



  }

}

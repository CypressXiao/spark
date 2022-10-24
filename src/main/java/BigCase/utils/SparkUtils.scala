package BigCase.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @projectName: spark 
 * @package: BigCase.utils
 * @objectName: SparkUtils
 * @author: Cypress_Xiao
 * @description: Spark工具类
 * @date: 2022/7/2 21:51
 * @version: 1.0
 */
object SparkUtils {
  def getSparkContext:SparkContext = {
    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("电商产品访问情况统计")
    new SparkContext(conf)
  }
}

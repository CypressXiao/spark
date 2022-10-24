package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: Transformations_Operator_Map
 * @author: Cypress_Xiao
 * @description: 转换算子map
 * @date: 2022/6/29 15:15
 * @version: 1.0
 */
object Transformations_Operator_Map {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("Map_Operator")
    val rdd1:RDD[String] = sc.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\data")
    val rdd2:RDD[String] = rdd1.map(line => line.toUpperCase)

    val nums:Int = rdd2.getNumPartitions
    println(nums)
    rdd2.mapPartitionsWithIndex((p, iters) => {
      iters.map(line => {
        line + "分区是" + p
      })
    }).foreach(println)
    rdd2.foreach(println)


  }

}

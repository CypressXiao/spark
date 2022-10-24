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
object Transformations_Operator_Demo {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("Operator_Demo")
    val rdd1:RDD[String] = sc.textFile("data")


    sc.stop()

  }

  def maxSubScore(rdd1:RDD[String]):Unit = {
    //计算每个科目的最高分
    rdd1
      .map(line => {
        val arr:Array[String] = line.split(",")
        (arr(0), arr(1), arr(2), arr(3).toDouble)
      })
      .groupBy(_._3)
      .map(e => {
        (e._1, e._2.map(_._4).max)
      })
      .foreach(println)
  }

  def getSubAvgScore(rdd1:RDD[String]):Unit = {
    //计算平均分
    rdd1
      .map(line => {
        val arr:Array[String] = line.split(",")
        (arr(0), arr(1), arr(2), arr(3).toDouble)
      })
      .groupBy(_._3)
      .map(e => {
        (e._1, e._2.map(_._4).sum / e._2.size)
      })
      .foreach(println)
  }

  def getTotalScore(rdd1:RDD[String]):Unit = {
    //求总分
    rdd1
      .map(line => {
        val arr:Array[String] = line.split(",")
        (arr(0), arr(1), arr(2), (arr(3).toDouble))
      })
      .groupBy(_._2)
      .map(e =>
        (e._1, e._2.map(_._4).sum))
      .foreach(println)
  }
}

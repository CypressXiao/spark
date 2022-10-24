package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: TestDemo1
 * @author: Cypress_Xiao
 * @description: 获取每组的年龄最大的人的信息  , 如果最大年龄 <18 岁  18岁 使用aggregateByKey
 * @date: 2022/7/1 19:14
 * @version: 1.0
 */
object TestDemo1 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("MAX_AGE")
    val lines:RDD[String] = sc.textFile("data/log.log")
    val per:RDD[(String, (Int, String, String, String, Int))] = lines.map(line => {
      val arr:Array[String] = line.split(",")
      (arr(3), (arr(0).toInt, arr(1), arr(2), arr(3), arr(4).toInt))
    })

    val res:RDD[(String, Int)] = per.aggregateByKey(18)((x1:Int, x2:(Int, String, String, String, Int)) => {
      if (x1 > x2._5 && x1 > 18) {
        x1
      } else if (x1 <= x2._5 && x2._5 > 18) {
        x2._5
      } else {
        18
      }
    },
      (x1:Int, x2:Int) => {
        if (x1 > x2) {
          x1
        } else {
          x2
        }
      })

    res.foreach(println)
  }
}

package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: ActionOperators
 * @author: Cypress_Xiao
 * @description: 行动算子
 * @date: 2022/7/2 14:31
 * @version: 1.0
 */
object ActionOperators {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("action", 4)
    val rdd:RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7))
    val num:Int = rdd.reduce(_ + _)
    println(num)
    println(rdd.count())
    println(rdd.takeOrdered(3).toList) //隐式传递myRul排序方法
  }

  implicit def myRul:Ordering[Int] = new Ordering[Int] {
    override def compare(x:Int, y:Int):Int = y - x
  }

}

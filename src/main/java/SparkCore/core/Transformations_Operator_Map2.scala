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
object Transformations_Operator_Map2 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("Map_Operator")
    val rdd1:RDD[String] = sc.textFile("C:\\Users\\Cypress_Xiao\\Desktop\\data")
    /**
     * map算子 内部函数 f 根据分区个数 在各个分区执行
     * 迭代所在分区内的每个元素 分别处理
     * f 运行在不同的机器上
     */
    val rdd2:RDD[String] = rdd1.map(line => line.toUpperCase)

    val rdd3:RDD[String] = rdd2.flatMap(_.split("\\s+"))
    rdd3.map((_,1)).map(tp =>(tp._1.toLowerCase,tp._2))




  }

}

package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: _累加器
 * @author: Cypress_Xiao
 * @description: 全局计数器
 * @date: 2022/7/4 15:38
 * @version: 1.0
 */
object _累加器 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("累加器", 8)
    val cntLines:LongAccumulator = sc.longAccumulator("cntLines")
    val dataRDD1:RDD[String] = sc.textFile("data/wordcount")
    //运算并触发行动算子
    dataRDD1.map(_ => {
      cntLines.add(1)
    }).collect()

    println(cntLines.value)

    sc.stop()
  }

}

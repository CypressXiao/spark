package SparkCore.core

import SparkCore.beans.RDDUserBean
import SparkCore.utils.SparkUtil
import com.alibaba.fastjson2.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: Transformations_Operator_LoadJson
 * @author: Cypress_Xiao
 * @description: 将数据转化为json
 * @date: 2022/6/29 15:15
 * @version: 1.0
 */
object Transformations_Operator_LoadJson {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("LoadJson")
    val rdd1:RDD[String] = sc.textFile("data/user")
    val rdd2:RDD[RDDUserBean] = rdd1.map(line => {
      try {
        JSON.parseObject(line, classOf[RDDUserBean])
      } catch {
        case _:Exception => null
      }
    })
    rdd2.filter(_ != null).foreach(println)

  }
}

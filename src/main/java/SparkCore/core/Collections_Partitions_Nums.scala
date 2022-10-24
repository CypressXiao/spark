package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: Partitions_Nums
 * @author: Cypress_Xiao
 * @description: 分区数验证
 * @date: 2022/6/29 11:05
 * @version: 1.0
 */
object Collections_Partitions_Nums {
  def main(args:Array[String]):Unit = {

    val sc:SparkContext = SparkUtil.getSparkContext("RDD分区数", 3)

    val ls:_root_.scala.collection.immutable.List[Int] = List[Int](1,2,3,4,5,6)
    val rdd:RDD[Int] = sc.makeRDD(ls,2)
    val nums:Int = rdd.getNumPartitions

    println(nums)

    sc.stop()





  }

}

package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * @projectName: spark 
 * @package: SparkDay01.core
 * @objectName: MakeRDD_File
 * @author: Cypress_Xiao
 * @description: 从文件中获取文件创建RDD
 * @date: 2022/6/28 17:51
 * @version: 1.0
 */

object MakeRDD_File {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("mkRDD_File", 2)
    //sequenceFile 首尾相连的KV数据 zss_23|lss_33
    //sc.sequenceFile("")

    //迭代的是每行数据
    val fileRDD:RDD[String] = sc.textFile("hdfs://hadoop001:8020/user/hive/warehouse/doit32.db/grade")

    val partition_nums:Int = fileRDD.getNumPartitions
    fileRDD.saveAsTextFile("partition_result")





  }

}

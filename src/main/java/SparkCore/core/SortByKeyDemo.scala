package SparkCore.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: SortByKeyDemo
 * @author: Cypress_Xiao
 * @description: SortByKey算子
 * @date: 2022/7/2 9:31
 * @version: 1.0
 */
object SortByKeyDemo {
  def main(args:Array[String]):Unit = {
    val conf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("SortByKey")
    val sc = new SparkContext(conf)
    val dataRDD:RDD[String] = sc.textFile("data/log.log")
    val tpRDD:RDD[(Int, (Int, String, String, String, Int))] = dataRDD.map(line => {
      val arr:Array[String] = line.split(",")
      (arr(4).toInt,(arr(0).toInt, arr(1), arr(2), arr(3), arr(4).toInt))
    })

    tpRDD.sortByKey().map(_._2).collect().foreach(println)




  }

}

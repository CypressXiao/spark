package SparkCore.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: CheckPointDemo
 * @author: Cypress_Xiao
 * @description: checkPoint
 * @date: 2022/7/7 16:08
 * @version: 1.0
 */
object CheckPointDemo {
  def main(args:Array[String]):Unit = {

    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("checkPoint")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("hdfs://hadoop001:8020/spark/wordcount")

    val dataRDD:RDD[String] = sc.textFile("data/wordcount")
    val rdd1:RDD[String] = dataRDD.flatMap(_.split(","))
    val rdd2:RDD[(String, Int)] = rdd1.map((_, 1))
    println(rdd2.toDebugString)
    rdd2.checkpoint()
    rdd2.collect()
    val res:RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    println(res.toDebugString)
    res.collect()

  }

}

package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: JoinDemo
 * @author: Cypress_Xiao
 * @description: TODO  
 * @date: 2022/7/2 10:57
 * @version: 1.0
 */
object JoinDemo {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("Join", 8)
    val lsRDD1:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 5, 6))
    val lsRDD2:RDD[String] = sc.makeRDD(List[String]("Java", "Python", "Scala", "Spark", "FLink", "Go"))
    val res:RDD[(Int, String)] = lsRDD1.zip(lsRDD2)
    res.foreach(println)

  }
}

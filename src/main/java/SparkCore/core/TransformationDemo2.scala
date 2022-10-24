package SparkCore.core

import SparkCore.utils.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @projectName: spark 
 * @package: SparkCore.core
 * @objectName: TransformationDemo2
 * @author: Cypress_Xiao
 * @description: 转换算子
 * @date: 2022/7/1 15:55
 * @version: 1.0
 */
object TransformationDemo2 {
  def main(args:Array[String]):Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("aggregateByKey")
    val dataRDD:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 6, 7), 2)
    val kvRDD:RDD[(String, Int)] = dataRDD.map(data => {
      if (data % 2 == 0) {
        ("OU", data)
      } else {
        ("JI", data)
      }
    })

    val res:RDD[(String, String)] = kvRDD.aggregateByKey("Test")((x1, x2) => x1 + x2, (x1, x2) => x1 + x2)
    res.foreach(println)

  }

  private def reduceByKey():Unit = {
    val sc:SparkContext = SparkUtil.getSparkContext("reduceByKey")
    val dataRDD:RDD[String] = sc.textFile("data/wordcount")
    val rdd:RDD[String] = dataRDD
      .flatMap(line => {
        line.split("\\s+")
      })
    val rdd1:RDD[(String, Int)] = rdd.map(e => (e, 1))


    rdd1.reduceByKey((x1, x2) => x1 + x2).foreach(println)
    println("***********************************************************")
    rdd1.foldByKey(0)((x1, x2) => x1 + x2).foreach(println)
    println("***********************************************************")
  }
}

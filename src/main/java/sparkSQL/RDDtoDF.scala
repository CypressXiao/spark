package sparkSQL

import SparkCore.beans.Hero
import SparkCore.utils.SparkUtil
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @projectName: spark 
 * @package: sparkSQL
 * @objectName: RDDtoDF
 * @author: Cypress_Xiao
 * @description: 各种RDD转DataFrame
 * @date: 2022/7/13 17:36
 * @version: 1.0
 */
object RDDtoDF {
  def main(args:Array[String]):Unit = {
    val sess:SparkSession = SparkUtil.getSession(this.getClass.getSimpleName)
    import sess.implicits._
    import org.apache.spark.sql.functions._
    val sc:SparkContext = sess.sparkContext
    val data:RDD[String] = sc.textFile("file:///D:\\AllContent\\spark\\data\\RDD")

    println("----------------------------tpRDDToDF------------------------------------------")
    val rdd1:RDD[(Int, String, Int, String, Double)] = data.map(line => {
      val arr:Array[String] = line.split(",")
      (arr(0).toInt, arr(1), arr(2).toInt, arr(3), arr(4).toDouble)
    })
    val df:DataFrame = rdd1.toDF("id", "name", "age", "city", "score")
    df.rdd.saveAsTextFile("/test",classOf[BZip2Codec])
    df.printSchema()
    df.show()

    println("----------------------------caseClassRDDToDF------------------------------------------")

    val rdd2:RDD[Hero] = data.map(line => {
      val arr:Array[String] = line.split(",")
      Hero(arr(0).toInt, arr(1), arr(2).toInt, arr(3), arr(4).toDouble)
    })
    val df1:DataFrame = rdd2.toDF()
    df1.printSchema()
    df1.show()


  }


}
